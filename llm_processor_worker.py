"""
llm_processor_worker.py
=======================
LLM Analysis Worker — AWS Bedrock Claude Haiku 4.5 + OpenAI GPT-4o-mini fallback

Fallback logic:
  Primary:  AWS Bedrock (Claude Haiku 4.5) — fast, cheap, cached
  Fallback: OpenAI GPT-4o-mini — when Bedrock hits daily token quota
  Both produce the same Flat TOON format output.

Flow:
  1. Scan temp/live-doc-history/ for meetings with done.json but no llm-done.json
  2. Extract base_prefix from done.json (handles ALL formats)
  3. Find REAL final S3 path — handles old path (mid inside path) and new path (mid at end)
  4. Check transcript exists — wait up to 60min with 6 retries
  5. Read doc.txt + TRANSCRIPT/*.vtt
  6. Split large doc.txt into chunks (if > MAX_DOC_CHUNK chars)
  7. Call LLM (Bedrock primary, OpenAI fallback) — each Part gets its own doc chunk
  8. Save llm.txt to final S3 path
  9. Write llm-done.json to temp

Doc chunking:
  If doc.txt > 150,000 chars → split into chunks
  Part 1 → Doc chunk 1 (oldest content)
  Part 2 → Doc chunk 2 (middle content)
  Part 3 → Doc chunk 3 (newest content — most relevant)
  OpenAI fallback → last 150K chars (most recent)

Path formats handled (find_real_base_prefix):
  NEW path: Interview-Success/Host/Year/Month/Candidate/Company/Date/Round/MeetingID/
  OLD path: Interview-Success/Host/Year/Month/Candidate/MeetingID/Company/Date/Round/Time/
  ANY path: if meeting_id appears anywhere in path, we scan S3 to find real TRANSCRIPT location

done.json formats supported:
  Format A (old finalizer):  "final_s3_prefix": "Interview-Success/..."
  Format B (new finalizer):  "final_s3_prefix" + "final_doc_txt" + "temp_prefix"
  Format C (llm format):     "base_prefix": "Interview-Success/..."
  Format D (fallback):       extract from "final_doc_txt" by stripping /docs/doc.txt

Departments supported:
  Interview-Success, Training, Customer-Success, Marketing
"""

import os
import sys
import json
import time
import logging
import threading
import boto3
import urllib.request
from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────────────────────────
AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET         = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
PROMPT_FILE       = os.environ.get("PROMPT_FILE",
                    "/home/ec2-user/google-docs-live/prompt.txt")

BEDROCK_MODEL_ID  = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
OPENAI_MODEL_ID   = "gpt-4o-mini"
MAX_OUTPUT_TOKENS = 8192
BACKFILL_WORKERS  = 30
SCAN_INTERVAL     = 120
IST_OFFSET        = timedelta(hours=5, minutes=30)

THROTTLE_ERRORS        = ("ThrottlingException", "Too many tokens", "rate limit", "quota")
MAX_TRANSCRIPT_RETRIES = 6
RETRY_WAIT_MINUTES     = 10
SMART_SKIP_THRESHOLD   = 7

# Large doc chunking — if doc.txt exceeds this, split into chunks
# 150K chars ≈ 37K tokens — safe for both Bedrock and OpenAI context windows
MAX_DOC_CHUNK = 150_000

# All top-level S3 departments to search when doing meeting_id scan
S3_DEPARTMENTS = [
    "Interview-Success",
    "Training",
    "Customer-Success",
    "Marketing",
]

# ── LOGGING ───────────────────────────────────────────────────────────────────
os.makedirs("/home/ec2-user/google-docs-live/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            "/home/ec2-user/google-docs-live/logs/llm_processor_worker.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

# ── AWS CLIENTS ───────────────────────────────────────────────────────────────
_boto_cfg = Config(
    retries={"max_attempts": 5, "mode": "adaptive"},
    read_timeout=600,
    connect_timeout=10,
    max_pool_connections=60,
)
s3c     = boto3.client("s3",               region_name=AWS_REGION, config=_boto_cfg)
sm      = boto3.client("secretsmanager",   region_name=AWS_REGION, config=_boto_cfg)
bedrock = boto3.client("bedrock-runtime",  region_name=AWS_REGION, config=_boto_cfg)

# ── SECRETS (cached) ──────────────────────────────────────────────────────────
_sec_cache = {}
_sec_lock  = threading.Lock()

def get_secret(name):
    with _sec_lock:
        if name not in _sec_cache:
            raw = sm.get_secret_value(SecretId=name)["SecretString"]
            _sec_cache[name] = json.loads(raw)
        return _sec_cache[name]

# ── PROMPT FILE (cached) ──────────────────────────────────────────────────────
_prompt      = None
_prompt_lock = threading.Lock()

def get_prompt():
    global _prompt
    with _prompt_lock:
        if _prompt is None:
            with open(PROMPT_FILE, "r", encoding="utf-8") as f:
                _prompt = f.read()
        return _prompt

# ── S3 HELPERS ────────────────────────────────────────────────────────────────

def s3_read(key):
    """Read S3 object as UTF-8 string. Returns '' on any error."""
    try:
        return s3c.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
    except Exception:
        return ""

def s3_put_json(key, data):
    s3c.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

def s3_put_text(key, text):
    s3c.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain",
    )

# ══════════════════════════════════════════════════════════════════════════════
# DOC CHUNKING — split large doc.txt into parts
# ══════════════════════════════════════════════════════════════════════════════

def split_doc_chunks(doc_text: str) -> list:
    """
    Split large doc.txt into chunks of MAX_DOC_CHUNK chars.
    Splits on newline boundaries to avoid cutting mid-line.

    Returns list of strings:
      - If doc fits in one chunk: [doc_text]
      - If doc is large: [chunk1, chunk2, chunk3, ...]

    Each Part call gets its assigned chunk:
      Part 1 → chunk 0 (oldest doc versions)
      Part 2 → chunk 1 (middle doc versions)
      Part 3 → last chunk (newest/most recent doc versions)
    """
    if len(doc_text) <= MAX_DOC_CHUNK:
        return [doc_text]

    chunks = []
    start  = 0
    while start < len(doc_text):
        end = start + MAX_DOC_CHUNK
        if end < len(doc_text):
            # Find nearest newline to avoid cutting mid-line
            nl = doc_text.rfind("\n", start, end)
            if nl > start:
                end = nl
        chunks.append(doc_text[start:end])
        start = end

    return chunks


def get_doc_chunk_for_part(chunks: list, part_num: int, total_parts: int = 3) -> str:
    """
    Get the right doc chunk for a given LLM part.
    Part 1 → first chunk (oldest content)
    Part 2 → middle chunk
    Part 3 → last chunk (newest/most relevant content)

    If there are more than 3 chunks, Part 3 always gets the last one.
    """
    if len(chunks) == 1:
        return chunks[0]

    if part_num == 1:
        return chunks[0]
    elif part_num == 3:
        return chunks[-1]
    else:  # Part 2
        mid = len(chunks) // 2
        return chunks[mid]

# ══════════════════════════════════════════════════════════════════════════════
# PATH RESOLUTION — handles EVERY path format
# ══════════════════════════════════════════════════════════════════════════════

def extract_base_prefix(done: dict) -> tuple:
    def clean(raw):
        return raw.replace(f"s3://{S3_BUCKET}/", "").rstrip("/").strip()

    bp = clean(done.get("base_prefix", ""))
    if bp:
        return bp, "base_prefix"

    bp = clean(done.get("final_s3_prefix", ""))
    if bp:
        return bp, "final_s3_prefix"

    doc_txt_url = done.get("final_doc_txt", "")
    if doc_txt_url:
        bp = clean(doc_txt_url)
        if bp.endswith("/docs/doc.txt"):
            bp = bp[: -len("/docs/doc.txt")]
        if bp:
            return bp, "final_doc_txt"

    return "", "none"


def meeting_id_at_end_of_prefix(prefix: str, meeting_id: str) -> bool:
    parts = prefix.rstrip("/").split("/")
    return parts[-1] == meeting_id


def find_real_base_prefix_by_scan(meeting_id: str) -> str:
    for dept in S3_DEPARTMENTS:
        paginator = s3c.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{dept}/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if meeting_id not in key:
                        continue
                    if not key.endswith(".vtt"):
                        continue
                    if "/TRANSCRIPT/" not in key:
                        continue
                    base = key.split("/TRANSCRIPT/")[0]
                    if base.rstrip("/").split("/")[-1] == meeting_id:
                        return base
                    return base
        except Exception as e:
            log.warning(f"S3 scan error in {dept}: {e}")
    return ""


def resolve_real_base_prefix(raw_prefix: str, meeting_id: str) -> str:
    if meeting_id_at_end_of_prefix(raw_prefix, meeting_id):
        return raw_prefix

    log.info(
        f"[{meeting_id}] Path does not end with meeting_id "
        f"(old format detected) — scanning S3 for real path..."
    )
    real = find_real_base_prefix_by_scan(meeting_id)
    if real:
        log.info(f"[{meeting_id}] ✅ Real path found via S3 scan: {real}")
        return real

    log.warning(
        f"[{meeting_id}] ⚠️  S3 scan found nothing — using raw prefix: {raw_prefix}"
    )
    return raw_prefix

# ══════════════════════════════════════════════════════════════════════════════
# TRANSCRIPT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def parse_vtt(vtt_text: str) -> str:
    lines = []
    for line in vtt_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("WEBVTT"):
            continue
        if "-->" in line:
            continue
        if line.isdigit():
            continue
        lines.append(line)
    return " ".join(lines)


def find_transcript_vtt(base_prefix: str) -> str:
    try:
        resp = s3c.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{base_prefix}/TRANSCRIPT/",
            MaxKeys=10,
        )
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".vtt"):
                return obj["Key"]
    except Exception as e:
        log.warning(f"Transcript list error [{base_prefix}]: {e}")
    return ""


def has_transcript(base_prefix: str) -> bool:
    key = find_transcript_vtt(base_prefix)
    if key:
        log.info(f"Transcript found: {key}")
        return True
    return False


def get_transcript_text(base_prefix: str) -> str:
    key = find_transcript_vtt(base_prefix)
    if not key:
        return ""
    try:
        vtt = s3_read(key)
        if vtt:
            return parse_vtt(vtt)
    except Exception as e:
        log.warning(f"VTT read error [{base_prefix}]: {e}")
    return ""

# ══════════════════════════════════════════════════════════════════════════════
# TRANSCRIPT WAIT LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def read_retry_state(temp_prefix: str) -> dict:
    raw = s3_read(f"{temp_prefix}/llm-retry.json")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def write_retry_state(temp_prefix: str, meeting_id: str, retries: int,
                      first_checked_at: str, base_prefix: str):
    now_utc = datetime.now(timezone.utc)
    s3_put_json(f"{temp_prefix}/llm-retry.json", {
        "meeting_id":             meeting_id,
        "retries":                retries,
        "max_retries":            MAX_TRANSCRIPT_RETRIES,
        "retry_interval_minutes": RETRY_WAIT_MINUTES,
        "first_checked_at":       first_checked_at,
        "last_checked_at":        now_utc.isoformat(),
        "last_checked_at_ist":    (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "base_prefix":            base_prefix,
        "next_retry_num":         retries + 1,
    })


def write_llm_error(temp_prefix: str, meeting_id: str,
                    base_prefix: str, retries: int):
    now_utc = datetime.now(timezone.utc)
    s3_put_json(f"{temp_prefix}/llm-error.json", {
        "meeting_id":        meeting_id,
        "status":            "transcript_not_found",
        "reason":            (
            f"TRANSCRIPT/*.vtt never appeared after {retries} retries "
            f"({retries * RETRY_WAIT_MINUTES} minutes total wait)"
        ),
        "retries_attempted": retries,
        "waited_minutes":    retries * RETRY_WAIT_MINUTES,
        "base_prefix":       base_prefix,
        "error_at":          now_utc.isoformat(),
        "error_at_ist":      (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "action":            "LLM skipped permanently — no transcript",
    })
    log.warning(
        f"[{meeting_id}] ❌ llm-error.json written — "
        f"transcript never arrived after {retries} retries "
        f"({retries * RETRY_WAIT_MINUTES} min)"
    )


def should_retry_now(state: dict) -> bool:
    last = state.get("last_checked_at")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
        elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60
        return elapsed >= RETRY_WAIT_MINUTES
    except Exception:
        return True


def transcript_wait_check(meeting_id: str, temp_prefix: str,
                           base_prefix: str) -> str:
    if has_transcript(base_prefix):
        log.info(f"[{meeting_id}] ✅ Transcript found — proceeding")
        return "go"

    state   = read_retry_state(temp_prefix)
    retries = state.get("retries", 0)
    now_utc = datetime.now(timezone.utc)

    if not should_retry_now(state):
        log.info(
            f"[{meeting_id}] ⏳ No transcript — too soon to retry "
            f"(retry {retries}/{MAX_TRANSCRIPT_RETRIES}, "
            f"last: {state.get('last_checked_at_ist', '?')})"
        )
        return "wait"

    retries += 1

    if retries >= MAX_TRANSCRIPT_RETRIES:
        write_llm_error(temp_prefix, meeting_id, base_prefix, retries)
        return "stop"

    first = state.get("first_checked_at", now_utc.isoformat())
    write_retry_state(temp_prefix, meeting_id, retries, first, base_prefix)
    log.info(
        f"[{meeting_id}] ⏳ No transcript — "
        f"retry {retries}/{MAX_TRANSCRIPT_RETRIES} "
        f"(next check in ~{RETRY_WAIT_MINUTES} min)"
    )
    return "wait"

# ══════════════════════════════════════════════════════════════════════════════
# S3 SCANNER
# ══════════════════════════════════════════════════════════════════════════════

def scan_s3_for_unprocessed() -> list:
    paginator     = s3c.get_paginator("list_objects_v2")
    has_done      = {}
    has_llm_done  = set()
    has_llm_error = set()

    for page in paginator.paginate(Bucket=S3_BUCKET,
                                   Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key   = obj["Key"]
            lm    = obj.get("LastModified")
            parts = key.split("/")

            # NEW: temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/mid/file
            if (len(parts) >= 7
                    and parts[2].isdigit() and len(parts[2]) == 4
                    and parts[3].startswith("Month-")):
                mid = parts[5]
                fn  = parts[6]
                pfx = "/".join(parts[:6])

            # OLD: temp/live-doc-history/mid/file
            elif len(parts) == 4 and parts[2].isdigit():
                mid = parts[2]
                fn  = parts[3]
                pfx = "/".join(parts[:3])

            else:
                continue

            if not mid.isdigit():
                continue

            if fn == "done.json":
                has_done[mid] = {"pfx": pfx, "lm": lm}
            elif fn == "llm-done.json":
                has_llm_done.add(mid)
            elif fn == "llm-error.json":
                has_llm_error.add(mid)

    pending = [
        {"mid": mid, "pfx": info["pfx"], "lm": info["lm"]}
        for mid, info in has_done.items()
        if mid not in has_llm_done
        and mid not in has_llm_error
    ]

    pending.sort(
        key=lambda x: x["lm"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    log.info(
        f"Scanner: {len(pending)} to process | "
        f"{len(has_done)} have done.json | "
        f"{len(has_llm_done)} already processed | "
        f"{len(has_llm_error)} permanently failed"
    )
    return pending

# ══════════════════════════════════════════════════════════════════════════════
# LLM — OPENAI (single call, 16K output)
# ══════════════════════════════════════════════════════════════════════════════

_openai_key      = None
_openai_key_lock = threading.Lock()

def get_openai_key() -> str:
    global _openai_key
    with _openai_key_lock:
        if _openai_key:
            return _openai_key
        sec = get_secret("secrets/api")
        key = sec.get("OPENAI_API_KEY", "")
        if not key:
            raise ValueError("OPENAI_API_KEY not found in secrets/api")
        _openai_key = key
        return _openai_key


def is_throttle_error(e: Exception) -> bool:
    msg = str(e)
    return any(t.lower() in msg.lower() for t in THROTTLE_ERRORS)


def call_openai_raw(messages: list) -> str:
    api_key = get_openai_key()
    payload = json.dumps({
        "model":      OPENAI_MODEL_ID,
        "max_tokens": 16000,
        "messages":   messages,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data["choices"][0]["message"]["content"]


def run_llm_openai(doc_text: str, transcript_text: str,
                   meeting_id: str) -> tuple:
    """
    Single-call OpenAI fallback.
    For large docs: use last MAX_DOC_CHUNK chars (most recent/relevant content).
    Returns (output_text, "openai")
    """
    prompt = get_prompt()

    # For large docs, OpenAI gets the most recent content (last chunk)
    if len(doc_text) > MAX_DOC_CHUNK:
        doc_for_llm = doc_text[-MAX_DOC_CHUNK:]
        log.info(
            f"[{meeting_id}] OpenAI: large doc truncated "
            f"{len(doc_text):,} → {MAX_DOC_CHUNK:,} chars (last/newest content)"
        )
    else:
        doc_for_llm = doc_text

    input_text = (
        f"DOCUMENT:\n{doc_for_llm}\n\n"
        f"TRANSCRIPT:\n{transcript_text if transcript_text else 'N/A'}"
    )
    now_utc = datetime.now(timezone.utc)
    header  = (
        f"LLM ANALYSIS REPORT\n"
        f"Meeting ID: {meeting_id}\n"
        f"Generated At: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC "
        f"({(now_utc + IST_OFFSET).strftime('%Y-%m-%d %I:%M:%S %p')} IST)\n"
        f"Model: GPT-4o-mini (OpenAI fallback)\n"
        f"\n{'=' * 60}\n"
    )
    log.info(f"[{meeting_id}] OpenAI single-call starting...")
    result = call_openai_raw([
        {"role": "system", "content": prompt},
        {
            "role": "user",
            "content": (
                f"{prompt}\n\n"
                f"Generate the COMPLETE analysis with ALL sections.\n\n"
                f"{input_text}"
            ),
        },
    ])
    log.info(f"[{meeting_id}] OpenAI single-call done ({len(result)} chars)")
    return header + result, "openai"

# ══════════════════════════════════════════════════════════════════════════════
# LLM — BEDROCK (3-part split + doc chunking for large docs)
# ══════════════════════════════════════════════════════════════════════════════

def call_bedrock_streaming(user_message: str, system_prompt: str) -> str:
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        MAX_OUTPUT_TOKENS,
        "system": [
            {
                "type":          "text",
                "text":          system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        "messages": [
            {"role": "user", "content": user_message}
        ],
    })

    response = bedrock.invoke_model_with_response_stream(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=body,
    )

    chunks = []
    stream = response.get("body")
    if stream:
        for event in stream:
            chunk = event.get("chunk")
            if chunk:
                data = json.loads(chunk.get("bytes", b"{}"))
                if data.get("type") == "content_block_delta":
                    delta = data.get("delta", {})
                    if delta.get("type") == "text_delta":
                        chunks.append(delta.get("text", ""))
    return "".join(chunks)


def run_llm_bedrock(doc_text: str, transcript_text: str,
                    meeting_id: str) -> tuple:
    """
    3-part Bedrock call with doc chunking for large docs.

    Normal doc (≤150K chars):
      All 3 parts get the full doc — same as before.

    Large doc (>150K chars):
      Split into chunks. Each Part gets its assigned chunk:
        Part 1 → chunk[0]  (oldest doc versions)
        Part 2 → chunk[mid] (middle doc versions)
        Part 3 → chunk[-1]  (newest/most recent doc versions)

    Smart skip: if Part 1 already has 7+/8 final sections → skip Parts 2+3.
    Returns (output_text, "bedrock")
    """
    prompt  = get_prompt()
    transcript = transcript_text if transcript_text else "N/A"

    now_utc = datetime.now(timezone.utc)
    header  = (
        f"LLM ANALYSIS REPORT\n"
        f"Meeting ID: {meeting_id}\n"
        f"Generated At: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC "
        f"({(now_utc + IST_OFFSET).strftime('%Y-%m-%d %I:%M:%S %p')} IST)\n"
        f"Model: Claude Haiku 4.5 (AWS Bedrock)\n"
        f"\n{'=' * 60}\n"
    )
    final_sections = [
        "overall_candidate_performance",
        "overall_proxy_support_performance",
        "overall_interviewer_performance",
        "chance_of_moving_forward",
        "if_chances_are_less_next_time_plan",
        "mistake_and_risk_ledger",
        "final_verdict",
        "insufficient_context",
    ]

    # ── Split doc into chunks if needed ───────────────────────────────────────
    doc_chunks = split_doc_chunks(doc_text)
    n_chunks   = len(doc_chunks)

    if n_chunks > 1:
        log.info(
            f"[{meeting_id}] Large doc ({len(doc_text):,} chars) → "
            f"{n_chunks} chunks of ~{MAX_DOC_CHUNK:,} chars each"
        )
    else:
        log.info(f"[{meeting_id}] Doc: {len(doc_text):,} chars (single chunk)")

    def make_input(part_num: int) -> str:
        """Build DOCUMENT + TRANSCRIPT input for a given part number."""
        doc_chunk = get_doc_chunk_for_part(doc_chunks, part_num)
        if n_chunks > 1:
            chunk_idx = {1: 0, 2: len(doc_chunks)//2, 3: len(doc_chunks)-1}[part_num]
            chunk_label = f"[DOC PART {chunk_idx+1}/{n_chunks} — "
            if part_num == 1:
                chunk_label += "OLDEST CONTENT]\n"
            elif part_num == 3:
                chunk_label += "NEWEST/MOST RECENT CONTENT]\n"
            else:
                chunk_label += "MIDDLE CONTENT]\n"
            doc_chunk = chunk_label + doc_chunk
        return (
            f"DOCUMENT:\n{doc_chunk}\n\n"
            f"TRANSCRIPT:\n{transcript}"
        )

    # ── Part 1 ────────────────────────────────────────────────────────────────
    log.info(f"[{meeting_id}] Bedrock Part 1 starting...")
    inp1  = make_input(1)
    part1 = call_bedrock_streaming(
        f"{prompt}\n\n"
        f"OUTPUT PART 1: Generate these sections only:\n"
        f"audit_metadata, audit_summary_card, role_identification, "
        f"flat_summary, overall_dynamics_of_interview, "
        f"question_answer_support_mapping\n\n"
        f"{inp1}",
        prompt,
    )
    log.info(f"[{meeting_id}] Part 1 done ({len(part1)} chars)")

    found = sum(1 for s in final_sections if s in part1)
    if found >= SMART_SKIP_THRESHOLD:
        log.info(f"[{meeting_id}] Smart skip: {found}/8 final sections in Part 1 ✅")
        return header + part1, "bedrock"

    # ── Part 2 ────────────────────────────────────────────────────────────────
    log.info(f"[{meeting_id}] Bedrock Part 2 starting...")
    inp2  = make_input(2)
    part2 = call_bedrock_streaming(
        f"{prompt}\n\n"
        f"CONTINUATION — Part 2.\n"
        f"Previous output (last 3000 chars):\n{part1[-3000:]}\n\n"
        f"OUTPUT PART 2: Generate ONLY missing sections:\n"
        f"overall_candidate_performance, overall_proxy_support_performance, "
        f"overall_interviewer_performance, chance_of_moving_forward\n"
        f"DO NOT regenerate any section already in Part 1.\n\n"
        f"{inp2}",
        prompt,
    )
    log.info(f"[{meeting_id}] Part 2 done ({len(part2)} chars)")

    combined = part1 + "\n" + part2
    found2   = sum(1 for s in final_sections if s in combined)
    if found2 >= SMART_SKIP_THRESHOLD:
        log.info(f"[{meeting_id}] Smart skip after Part 2: {found2}/8 ✅")
        return header + combined, "bedrock"

    # ── Part 3 ────────────────────────────────────────────────────────────────
    log.info(f"[{meeting_id}] Bedrock Part 3 starting...")
    inp3  = make_input(3)
    part3 = call_bedrock_streaming(
        f"{prompt}\n\n"
        f"CONTINUATION — Part 3 (final).\n"
        f"Previous output (last 2000 chars):\n{combined[-2000:]}\n\n"
        f"OUTPUT PART 3: Generate ONLY missing sections:\n"
        f"if_chances_are_less_next_time_plan, mistake_and_risk_ledger, "
        f"final_verdict, insufficient_context\n"
        f"DO NOT regenerate any section already written. "
        f"STOP after insufficient_context.\n\n"
        f"{inp3}",
        prompt,
    )
    log.info(f"[{meeting_id}] Part 3 done ({len(part3)} chars)")
    return header + combined + "\n" + part3, "bedrock"


def run_llm_3part(doc_text: str, transcript_text: str,
                  meeting_id: str) -> tuple:
    """
    Master LLM runner:
      Try Bedrock (3-part split + doc chunking) → on throttle → OpenAI fallback
    Returns (output_text, provider)
    """
    # Primary: Bedrock
    try:
        return run_llm_bedrock(doc_text, transcript_text, meeting_id)
    except Exception as e:
        if is_throttle_error(e):
            log.warning(
                f"[{meeting_id}] Bedrock throttled — "
                f"falling back to OpenAI GPT-4o-mini (single call)"
            )
        else:
            raise

    # Fallback: OpenAI
    try:
        return run_llm_openai(doc_text, transcript_text, meeting_id)
    except Exception as e:
        log.error(f"[{meeting_id}] OpenAI fallback also failed: {e}")
        raise

# ══════════════════════════════════════════════════════════════════════════════
# CORE: PROCESS ONE MEETING
# ══════════════════════════════════════════════════════════════════════════════

def process_one_meeting(item: dict) -> str:
    mid = item["mid"]
    pfx = item["pfx"]
    log.info(f"[{mid}] ── Processing ──")

    # ── Step 1: Read done.json ────────────────────────────────────────────────
    done_raw = s3_read(f"{pfx}/done.json")
    if not done_raw:
        return f"SKIP {mid} — done.json missing"
    try:
        done = json.loads(done_raw)
    except Exception:
        return f"SKIP {mid} — done.json parse error"

    # ── Step 2: Stub done.json check (no path) ────────────────────────────────
    raw_prefix, key_used = extract_base_prefix(done)
    if not raw_prefix:
        # Stub done.json — no Google Doc activity, nothing to process
        now_utc = datetime.now(timezone.utc)
        s3_put_json(f"{pfx}/llm-error.json", {
            "meeting_id":   mid,
            "status":       "skipped_permanently",
            "reason":       f"Stub done.json — no S3 path ({done.get('reason', 'no_doc_activity')})",
            "done_keys":    list(done.keys()),
            "error_at":     now_utc.isoformat(),
            "error_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        })
        log.info(f"[{mid}] ⏭️  Stub done.json — llm-error.json written, skip forever")
        return f"SKIP {mid} — stub done.json (no path), skipped permanently"

    log.info(f"[{mid}] raw_prefix='{raw_prefix}' (from '{key_used}')")

    # ── Step 3: Resolve REAL base_prefix ──────────────────────────────────────
    base_prefix = resolve_real_base_prefix(raw_prefix, mid)
    log.info(f"[{mid}] base_prefix='{base_prefix}'")

    # ── Step 4: Transcript wait check ─────────────────────────────────────────
    t_status = transcript_wait_check(mid, pfx, base_prefix)

    if t_status == "wait":
        return f"WAIT {mid} — transcript not ready, retry in ~{RETRY_WAIT_MINUTES}min"
    if t_status == "stop":
        return (
            f"ERROR {mid} — transcript never arrived after "
            f"{MAX_TRANSCRIPT_RETRIES} retries "
            f"({MAX_TRANSCRIPT_RETRIES * RETRY_WAIT_MINUTES} min)"
        )

    # ── Step 5: Read doc.txt ──────────────────────────────────────────────────
    doc_txt = s3_read(f"{base_prefix}/docs/doc.txt")
    if not doc_txt:
        log.warning(f"[{mid}] doc.txt not found at {base_prefix}/docs/doc.txt — trying raw path")
        doc_txt = s3_read(f"{raw_prefix}/docs/doc.txt")
    if not doc_txt:
        return f"SKIP {mid} — doc.txt not found at either path"

    log.info(f"[{mid}] doc.txt: {len(doc_txt):,} chars")

    # ── Step 6: Read transcript ───────────────────────────────────────────────
    transcript_text = get_transcript_text(base_prefix)
    log.info(
        f"[{mid}] Transcript: {len(transcript_text):,} chars"
        f"{' (empty)' if not transcript_text else ''}"
    )

    # ── Step 7: Run LLM ───────────────────────────────────────────────────────
    log.info(f"[{mid}] Starting LLM analysis...")
    try:
        llm_output, provider = run_llm_3part(doc_txt, transcript_text, mid)
    except Exception as e:
        log.error(f"[{mid}] LLM call failed: {e}", exc_info=True)
        return f"ERROR {mid} — LLM failed: {e}"

    if not llm_output or len(llm_output) < 100:
        return f"ERROR {mid} — LLM returned empty output ({len(llm_output)} chars)"

    log.info(f"[{mid}] LLM output: {len(llm_output):,} chars via {provider}")

    # ── Step 8: Save llm.txt ──────────────────────────────────────────────────
    llm_key = f"{base_prefix}/llm/llm.txt"
    s3_put_text(llm_key, llm_output)
    log.info(f"[{mid}] ✅ llm.txt saved → {llm_key}")

    # ── Step 9: Write llm-done.json ───────────────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    model_str = (
        "claude-haiku-4-5 (AWS Bedrock)" if provider == "bedrock"
        else "gpt-4o-mini (OpenAI)"       if provider == "openai"
        else f"mixed ({provider})"
    )
    doc_chunks_count = len(split_doc_chunks(doc_txt))
    s3_put_json(f"{pfx}/llm-done.json", {
        "meeting_id":       mid,
        "status":           "llm_processed",
        "processed_at":     now_utc.isoformat(),
        "processed_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "model":            model_str,
        "provider":         provider,
        "llm_txt":          f"s3://{S3_BUCKET}/{llm_key}",
        "doc_source":       f"s3://{S3_BUCKET}/{base_prefix}/docs/doc.txt",
        "doc_chars":        len(doc_txt),
        "doc_chunks":       doc_chunks_count,
        "transcript":       "found" if transcript_text else "N/A",
        "base_prefix":      base_prefix,
        "raw_prefix":       raw_prefix,
        "temp_prefix":      pfx,
        "output_chars":     len(llm_output),
    })
    log.info(f"[{mid}] ✅ llm-done.json written")

    # ── Step 10: Cost estimate ────────────────────────────────────────────────
    input_tokens  = (len(doc_txt) + len(transcript_text)) // 4
    output_tokens = len(llm_output) // 4
    if provider == "bedrock":
        est_cost = (input_tokens / 1_000_000 * 0.80) + (output_tokens / 1_000_000 * 4.00)
    else:
        est_cost = (input_tokens / 1_000_000 * 0.15) + (output_tokens / 1_000_000 * 0.60)

    log.info(
        f"[{mid}] 💰 ~{input_tokens:,} in / ~{output_tokens:,} out "
        f"/ est ~${est_cost:.4f} / provider={provider}"
        f"{f' / doc_chunks={doc_chunks_count}' if doc_chunks_count > 1 else ''}"
    )

    return f"OK {mid} | {len(llm_output):,} chars | ${est_cost:.4f} | {provider}"

# ══════════════════════════════════════════════════════════════════════════════
# BACKFILL LOOP
# ══════════════════════════════════════════════════════════════════════════════

def backfill_loop():
    log.info("Backfill loop started")
    while True:
        try:
            pending = scan_s3_for_unprocessed()

            if not pending:
                log.info(f"Nothing to process. Sleeping {SCAN_INTERVAL}s")
                time.sleep(SCAN_INTERVAL)
                continue

            log.info(
                f"Processing {len(pending)} meetings "
                f"with {BACKFILL_WORKERS} workers..."
            )

            with ThreadPoolExecutor(
                max_workers=BACKFILL_WORKERS,
                thread_name_prefix="llm-worker",
            ) as ex:
                futures = {
                    ex.submit(process_one_meeting, item): item["mid"]
                    for item in pending
                }
                for future in as_completed(futures):
                    mid = futures[future]
                    try:
                        result = future.result()
                        if result.startswith("OK"):
                            log.info(f"✅ {result}")
                        elif result.startswith("WAIT"):
                            log.info(f"⏳ WAIT {mid} — transcript not ready, retry in ~10min")
                        elif result.startswith("ERROR"):
                            log.warning(f"❌ {result}")
                        else:
                            log.info(f"⏭️  {result}")
                    except Exception as e:
                        log.error(f"Worker error [{mid}]: {e}", exc_info=True)

            log.info(f"Batch done. Sleeping {SCAN_INTERVAL}s")
            time.sleep(SCAN_INTERVAL)

        except Exception as e:
            log.error(f"Backfill loop error: {e}", exc_info=True)
            time.sleep(30)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("llm_processor_worker starting")
    log.info(f"  Primary model:      {BEDROCK_MODEL_ID}")
    log.info(f"  Fallback model:     {OPENAI_MODEL_ID}")
    log.info(f"  Workers:            {BACKFILL_WORKERS}")
    log.info(f"  Max output tokens:  {MAX_OUTPUT_TOKENS} (Bedrock) / 16000 (OpenAI)")
    log.info(f"  Smart skip at:      {SMART_SKIP_THRESHOLD}/8 sections")
    log.info(f"  Transcript retries: {MAX_TRANSCRIPT_RETRIES} × {RETRY_WAIT_MINUTES}min")
    log.info(f"  Scan interval:      {SCAN_INTERVAL}s")
    log.info(f"  Max doc chunk:      {MAX_DOC_CHUNK:,} chars")
    log.info(f"  Path resolution:    auto (new + old + S3 scan fallback)")
    log.info("=" * 60)

    try:
        p = get_prompt()
        log.info(f"Prompt loaded: {len(p):,} chars ✅")
    except Exception as e:
        log.error(f"Prompt file failed: {e}")
        sys.exit(1)

    try:
        bedrock.list_foundation_models(byProvider="anthropic")
        log.info("Bedrock access ✅")
    except Exception as e:
        log.warning(f"Bedrock check failed (may still work): {e}")

    backfill_loop()


if __name__ == "__main__":
    main()
"""
llm_processor_worker.py
=======================
LLM Analysis Worker — AWS Bedrock Claude Haiku 4.5 + OpenAI GPT-4o-mini fallback

Architecture (senior-engineer pattern):
  Tier 1: Single call (default — 99% of meetings)
          Input fits in 200K context window (~720K chars after prompt+output reserve)
          Faster, cheaper, more coherent output
  Tier 2: Map-reduce (only when input > 600K chars)
          MAP:    Split into time-aligned chunks → extract raw facts per chunk
          REDUCE: Send aggregated facts back → emit final strict v3.1 output
  Tier 3: OpenAI GPT-4o-mini fallback (when Bedrock throttles)

Why no more 3-part split for everyone:
  - Old prompt v3.0 produced 30K+ chars of output → forced 3-part split
  - New prompt v3.1 produces 3-5K chars of output → fits in one 8K-token response
  - 200K input context easily holds full doc + full transcript for any real meeting
  - Single call = better cross-reference, faster, cheaper

Output format (strict v3.1):
  ### Table 1: Proxy Support Performance Table
  ### Table 2: Candidate Performance Table
  #### Response Speed Analysis (bullets)
  #### Proxy Flag Details (bullets)
  #### Candidate Flag Details (bullets)
  #### Interviewer Engagement Summary
  #### Session Overview (bullets)
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

# Output token limits (max for each model — never truncate)
BEDROCK_MAX_OUTPUT_TOKENS = 8192    # Claude Haiku 4.5 hard limit
OPENAI_MAX_OUTPUT_TOKENS  = 16384   # GPT-4o-mini hard limit

# Input thresholds (in chars, ~4 chars per token)
# Claude Haiku 4.5 has 200K token context = ~800K chars
# Reserve: 11.5K tokens prompt + 8.2K output + 0.5K overhead = ~20K tokens
# Available for input: ~180K tokens = ~720K chars
# Set conservative threshold at 600K to leave buffer for prompt-cache misses
SINGLE_CALL_INPUT_LIMIT = 600_000   # chars — single call up to this size
MAP_REDUCE_CHUNK_SIZE   = 200_000   # chars per chunk in map-reduce mode
OPENAI_FALLBACK_INPUT_LIMIT = 480_000  # GPT-4o-mini 128K context = ~512K chars

BACKFILL_WORKERS  = 30
SCAN_INTERVAL     = 120
IST_OFFSET        = timedelta(hours=5, minutes=30)

THROTTLE_ERRORS        = ("ThrottlingException", "Too many tokens", "rate limit", "quota")
MAX_TRANSCRIPT_RETRIES = 6
RETRY_WAIT_MINUTES     = 10

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
# PATH RESOLUTION — handles every path format
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

            if (len(parts) >= 7
                    and parts[2].isdigit() and len(parts[2]) == 4
                    and parts[3].startswith("Month-")):
                mid = parts[5]
                fn  = parts[6]
                pfx = "/".join(parts[:6])
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
        if mid not in has_llm_done and mid not in has_llm_error
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
# CHUNKING — ONLY USED FOR MAP-REDUCE MODE (input > 600K chars)
# ══════════════════════════════════════════════════════════════════════════════

def split_by_size(text: str, max_chars: int) -> list:
    """Split text into chunks of max_chars, preferring newline boundaries."""
    if len(text) <= max_chars:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars
        if end < len(text):
            nl = text.rfind("\n", start, end)
            if nl > start + max_chars // 2:  # don't cut too short
                end = nl
        chunks.append(text[start:end])
        start = end
    return chunks


def split_for_map_reduce(doc_text: str, transcript_text: str) -> list:
    """
    For very large inputs (>600K chars total), split into time-aligned chunks.
    Each chunk gets a slice of doc + a slice of transcript covering same time window.

    Returns list of (doc_chunk, transcript_chunk, label) tuples.
    """
    total = len(doc_text) + len(transcript_text)
    if total <= SINGLE_CALL_INPUT_LIMIT:
        return [(doc_text, transcript_text, "FULL")]

    # Determine number of chunks needed (each chunk ≤ MAP_REDUCE_CHUNK_SIZE total)
    n_chunks = max(2, (total + MAP_REDUCE_CHUNK_SIZE - 1) // MAP_REDUCE_CHUNK_SIZE)
    n_chunks = min(n_chunks, 5)  # cap at 5 chunks to bound cost

    # Split each proportionally
    doc_chunks        = split_by_size(doc_text, len(doc_text) // n_chunks + 1)
    transcript_chunks = split_by_size(transcript_text, len(transcript_text) // n_chunks + 1)

    # Pad shorter list with empty strings so they zip evenly
    while len(doc_chunks) < n_chunks:
        doc_chunks.append("")
    while len(transcript_chunks) < n_chunks:
        transcript_chunks.append("")

    result = []
    for i in range(n_chunks):
        if i == 0:
            label = f"EARLY (1/{n_chunks})"
        elif i == n_chunks - 1:
            label = f"LATE ({n_chunks}/{n_chunks})"
        else:
            label = f"MID ({i+1}/{n_chunks})"
        result.append((doc_chunks[i], transcript_chunks[i], label))
    return result

# ══════════════════════════════════════════════════════════════════════════════
# OPENAI FALLBACK
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
        "max_tokens": OPENAI_MAX_OUTPUT_TOKENS,
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
    OpenAI single-call fallback. For oversized input, truncates to last
    OPENAI_FALLBACK_INPUT_LIMIT chars (most recent content).
    Returns (output_text, "openai")
    """
    prompt = get_prompt()

    total = len(doc_text) + len(transcript_text)
    if total > OPENAI_FALLBACK_INPUT_LIMIT:
        # Reserve half for transcript, half for doc
        per_side = OPENAI_FALLBACK_INPUT_LIMIT // 2
        doc_for_llm        = doc_text[-per_side:]
        transcript_for_llm = transcript_text[-per_side:]
        log.info(
            f"[{meeting_id}] OpenAI: input {total:,} > {OPENAI_FALLBACK_INPUT_LIMIT:,} "
            f"chars — truncated to last {per_side:,} per side"
        )
    else:
        doc_for_llm        = doc_text
        transcript_for_llm = transcript_text if transcript_text else "N/A"

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
                f"Analyze this interview session and produce the output following "
                f"STRICT_OUTPUT_FORMAT_ENFORCEMENT exactly.\n\n"
                f"MANDATORY: Start your response with '### Table 1:' — no preamble, "
                f"no analysis steps, no audit headers, no PART 1/2/3 labels.\n\n"
                f"OUTPUT EVERYTHING FULLY. Do not truncate. List every Q&A pair, "
                f"every flagged category, every piece of evidence.\n\n"
                f"DOCUMENT:\n{doc_for_llm}\n\n"
                f"TRANSCRIPT:\n{transcript_for_llm}"
            ),
        },
    ])
    log.info(f"[{meeting_id}] OpenAI single-call done ({len(result)} chars)")
    return header + result, "openai"

# ══════════════════════════════════════════════════════════════════════════════
# BEDROCK CALL
# ══════════════════════════════════════════════════════════════════════════════

def call_bedrock_streaming(user_message: str, system_prompt: str,
                            max_tokens: int = None) -> str:
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        max_tokens or BEDROCK_MAX_OUTPUT_TOKENS,
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


# ══════════════════════════════════════════════════════════════════════════════
# TIER 1: SINGLE-CALL BEDROCK (default, fits 99% of meetings)
# ══════════════════════════════════════════════════════════════════════════════

def run_bedrock_single_call(doc_text: str, transcript_text: str,
                              meeting_id: str) -> tuple:
    """
    Single Bedrock call with FULL doc + FULL transcript.
    Used when input <= SINGLE_CALL_INPUT_LIMIT (600K chars).
    Emits full strict v3.1 output.
    Returns (output_text, "bedrock")
    """
    prompt     = get_prompt()
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

    total = len(doc_text) + len(transcript_text)
    log.info(
        f"[{meeting_id}] Bedrock single-call: doc={len(doc_text):,} + "
        f"transcript={len(transcript_text):,} = {total:,} chars"
    )

    result = call_bedrock_streaming(
        user_message=(
            f"Analyze this interview session and produce the output following "
            f"STRICT_OUTPUT_FORMAT_ENFORCEMENT exactly.\n\n"
            f"MANDATORY: Start your response with '### Table 1:' — no preamble, "
            f"no analysis steps, no audit headers, no PART 1/2/3 labels.\n\n"
            f"Emit the FULL strict format COMPLETELY:\n"
            f"  ### Table 1: Proxy Support Performance Table\n"
            f"  ### Table 2: Candidate Performance Table\n"
            f"  #### Response Speed Analysis (list EVERY Q&A pair)\n"
            f"  #### Proxy Flag Details (list EVERY flagged category)\n"
            f"  #### Candidate Flag Details (list EVERY flagged category)\n"
            f"  #### Interviewer Engagement Summary (3-5 sentences)\n"
            f"  #### Session Overview (all 6 metrics as bullets)\n\n"
            f"OUTPUT EVERYTHING FULLY. Do not truncate, do not abbreviate. "
            f"You have 8192 tokens — use them as needed for complete output.\n\n"
            f"DOCUMENT:\n{doc_text}\n\n"
            f"TRANSCRIPT:\n{transcript}"
        ),
        system_prompt=prompt,
    )
    log.info(f"[{meeting_id}] Bedrock single-call done ({len(result)} chars)")
    return header + result, "bedrock"


# ══════════════════════════════════════════════════════════════════════════════
# TIER 2: MAP-REDUCE BEDROCK (only for input > 600K chars)
# ══════════════════════════════════════════════════════════════════════════════

def run_bedrock_map_reduce(doc_text: str, transcript_text: str,
                             meeting_id: str) -> tuple:
    """
    Map-reduce for very large inputs (>600K chars).

    MAP phase:
      For each (doc_chunk, transcript_chunk, label) — call Bedrock to extract
      raw facts: Q&A pairs, candidate evidence, proxy evidence (no scoring,
      no tables, no opinions — just facts with timestamps).

    REDUCE phase:
      Send aggregated facts back to Bedrock with full strict prompt to emit
      the final tables + sections.

    This pattern preserves coherent reasoning at the reduce step while
    breaking the input where it actually exceeds limits.

    Returns (output_text, "bedrock")
    """
    prompt = get_prompt()
    chunks = split_for_map_reduce(doc_text, transcript_text)
    n      = len(chunks)

    log.info(
        f"[{meeting_id}] Map-reduce mode: {len(doc_text)+len(transcript_text):,} "
        f"chars > {SINGLE_CALL_INPUT_LIMIT:,} → splitting into {n} chunks"
    )

    # ── MAP PHASE ─────────────────────────────────────────────────────────────
    map_facts = []
    for i, (doc_c, trans_c, label) in enumerate(chunks, 1):
        log.info(f"[{meeting_id}] Map {i}/{n} ({label}): "
                 f"doc={len(doc_c):,} + transcript={len(trans_c):,} chars")

        map_instruction = (
            f"Fact-extraction phase {i}/{n} (chunk: {label}).\n\n"
            f"Extract RAW FACTS only from this chunk. NO scoring, NO tables, "
            f"NO summaries, NO opinions, NO action_required values. "
            f"Just observed facts with timestamps.\n\n"
            f"Output format (markdown bullets ONLY):\n\n"
            f"#### Q&A Pairs (chunk {label})\n"
            f"- Q: <question text> | asked_at: HH:MM:SS | pasted_at: HH:MM:SS | "
            f"delta: N seconds | domain: <domain>\n\n"
            f"#### Candidate Observations (chunk {label})\n"
            f"- VTT HH:MM:SS | category: <closest enum from CANDIDATE_ACTION_CATEGORIES_ENUM> | "
            f"observation: <what was said/done>\n\n"
            f"#### Proxy Observations (chunk {label})\n"
            f"- Version N | category: <closest enum from PROXY_ACTION_CATEGORIES_ENUM> | "
            f"observation: <what was pasted/missed>\n\n"
            f"#### Interviewer Signals (chunk {label})\n"
            f"- HH:MM:SS | signal: <description>\n\n"
            f"Use ONLY enum strings for categories. Output ONLY these 4 sections, "
            f"nothing else. No tables, no scoring.\n\n"
            f"DOCUMENT chunk:\n{doc_c}\n\n"
            f"TRANSCRIPT chunk:\n{trans_c if trans_c else 'N/A'}"
        )
        try:
            facts = call_bedrock_streaming(
                user_message=map_instruction,
                system_prompt=prompt,
            )
            map_facts.append(f"=== CHUNK {label} ===\n{facts}")
            log.info(f"[{meeting_id}] Map {i}/{n} done ({len(facts):,} chars)")
        except Exception as e:
            log.warning(f"[{meeting_id}] Map {i}/{n} failed: {e} — continuing")

    if not map_facts:
        raise RuntimeError(f"[{meeting_id}] Map phase produced no facts")

    # ── REDUCE PHASE ──────────────────────────────────────────────────────────
    aggregated = "\n\n".join(map_facts)
    log.info(
        f"[{meeting_id}] Reduce phase starting: "
        f"{len(aggregated):,} chars of aggregated facts"
    )

    # If aggregated facts are still too large, truncate (rare)
    if len(aggregated) > SINGLE_CALL_INPUT_LIMIT:
        log.warning(
            f"[{meeting_id}] Aggregated facts {len(aggregated):,} > "
            f"{SINGLE_CALL_INPUT_LIMIT:,} — truncating to last N chars"
        )
        aggregated = aggregated[-SINGLE_CALL_INPUT_LIMIT:]

    reduce_instruction = (
        f"Synthesis phase. Below are extracted facts from {n} chunks of a single "
        f"interview session. Produce the FINAL output following "
        f"STRICT_OUTPUT_FORMAT_ENFORCEMENT exactly.\n\n"
        f"MANDATORY: Start with '### Table 1:' — no preamble.\n\n"
        f"Emit the FULL strict format:\n"
        f"  ### Table 1: Proxy Support Performance Table\n"
        f"  ### Table 2: Candidate Performance Table\n"
        f"  #### Response Speed Analysis (consolidated from all chunks)\n"
        f"  #### Proxy Flag Details (consolidated)\n"
        f"  #### Candidate Flag Details (consolidated)\n"
        f"  #### Interviewer Engagement Summary\n"
        f"  #### Session Overview\n\n"
        f"Aggregate evidence across chunks. Compute scores from the full picture. "
        f"Apply all 14 scoring rules from the prompt to the consolidated facts. "
        f"Use ONLY enum strings. OUTPUT EVERYTHING FULLY.\n\n"
        f"AGGREGATED FACTS FROM ALL CHUNKS:\n\n{aggregated}"
    )

    final = call_bedrock_streaming(
        user_message=reduce_instruction,
        system_prompt=prompt,
    )

    now_utc = datetime.now(timezone.utc)
    header  = (
        f"LLM ANALYSIS REPORT\n"
        f"Meeting ID: {meeting_id}\n"
        f"Generated At: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC "
        f"({(now_utc + IST_OFFSET).strftime('%Y-%m-%d %I:%M:%S %p')} IST)\n"
        f"Model: Claude Haiku 4.5 (AWS Bedrock — map-reduce {n} chunks)\n"
        f"\n{'=' * 60}\n"
    )
    log.info(f"[{meeting_id}] Reduce done ({len(final):,} chars)")
    return header + final, "bedrock"


# ══════════════════════════════════════════════════════════════════════════════
# MASTER LLM RUNNER (Tier 1 → Tier 2 → Tier 3)
# ══════════════════════════════════════════════════════════════════════════════

def run_llm(doc_text: str, transcript_text: str, meeting_id: str) -> tuple:
    """
    Master LLM runner.
      Tier 1: Single-call Bedrock (default — most meetings)
      Tier 2: Map-reduce Bedrock (only when input > 600K chars)
      Tier 3: OpenAI fallback (when Bedrock throttles)
    Returns (output_text, provider)
    """
    total_input = len(doc_text) + len(transcript_text)

    # Choose Bedrock strategy by input size
    if total_input <= SINGLE_CALL_INPUT_LIMIT:
        strategy = "single-call"
        try:
            return run_bedrock_single_call(doc_text, transcript_text, meeting_id)
        except Exception as e:
            if is_throttle_error(e):
                log.warning(
                    f"[{meeting_id}] Bedrock {strategy} throttled — "
                    f"falling back to OpenAI"
                )
            else:
                raise
    else:
        strategy = "map-reduce"
        log.info(
            f"[{meeting_id}] Input {total_input:,} chars > "
            f"{SINGLE_CALL_INPUT_LIMIT:,} threshold — using map-reduce"
        )
        try:
            return run_bedrock_map_reduce(doc_text, transcript_text, meeting_id)
        except Exception as e:
            if is_throttle_error(e):
                log.warning(
                    f"[{meeting_id}] Bedrock {strategy} throttled — "
                    f"falling back to OpenAI"
                )
            else:
                raise

    # Fallback: OpenAI
    try:
        return run_llm_openai(doc_text, transcript_text, meeting_id)
    except Exception as e:
        log.error(f"[{meeting_id}] OpenAI fallback also failed: {e}")
        raise


# Legacy alias — keep for backwards compatibility
run_llm_3part = run_llm


# ══════════════════════════════════════════════════════════════════════════════
# CORE: PROCESS ONE MEETING
# ══════════════════════════════════════════════════════════════════════════════

def process_one_meeting(item: dict) -> str:
    mid = item["mid"]
    pfx = item["pfx"]
    log.info(f"[{mid}] ── Processing ──")

    done_raw = s3_read(f"{pfx}/done.json")
    if not done_raw:
        return f"SKIP {mid} — done.json missing"
    try:
        done = json.loads(done_raw)
    except Exception:
        return f"SKIP {mid} — done.json parse error"

    raw_prefix, key_used = extract_base_prefix(done)
    if not raw_prefix:
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

    base_prefix = resolve_real_base_prefix(raw_prefix, mid)
    log.info(f"[{mid}] base_prefix='{base_prefix}'")

    t_status = transcript_wait_check(mid, pfx, base_prefix)
    if t_status == "wait":
        return f"WAIT {mid} — transcript not ready, retry in ~{RETRY_WAIT_MINUTES}min"
    if t_status == "stop":
        return (
            f"ERROR {mid} — transcript never arrived after "
            f"{MAX_TRANSCRIPT_RETRIES} retries "
            f"({MAX_TRANSCRIPT_RETRIES * RETRY_WAIT_MINUTES} min)"
        )

    doc_txt = s3_read(f"{base_prefix}/docs/doc.txt")
    if not doc_txt:
        log.warning(f"[{mid}] doc.txt not found at {base_prefix}/docs/doc.txt — trying raw path")
        doc_txt = s3_read(f"{raw_prefix}/docs/doc.txt")
    if not doc_txt:
        return f"SKIP {mid} — doc.txt not found at either path"

    log.info(f"[{mid}] doc.txt: {len(doc_txt):,} chars")

    transcript_text = get_transcript_text(base_prefix)
    log.info(
        f"[{mid}] Transcript: {len(transcript_text):,} chars"
        f"{' (empty)' if not transcript_text else ''}"
    )

    log.info(f"[{mid}] Starting LLM analysis...")
    try:
        llm_output, provider = run_llm(doc_txt, transcript_text, mid)
    except Exception as e:
        log.error(f"[{mid}] LLM call failed: {e}", exc_info=True)
        return f"ERROR {mid} — LLM failed: {e}"

    if not llm_output or len(llm_output) < 100:
        return f"ERROR {mid} — LLM returned empty output ({len(llm_output)} chars)"

    log.info(f"[{mid}] LLM output: {len(llm_output):,} chars via {provider}")

    llm_key = f"{base_prefix}/llm/llm.txt"
    s3_put_text(llm_key, llm_output)
    log.info(f"[{mid}] ✅ llm.txt saved → {llm_key}")

    now_utc = datetime.now(timezone.utc)
    model_str = (
        "claude-haiku-4-5 (AWS Bedrock)" if provider == "bedrock"
        else "gpt-4o-mini (OpenAI)"       if provider == "openai"
        else f"mixed ({provider})"
    )
    total_input = len(doc_txt) + len(transcript_text)
    s3_put_json(f"{pfx}/llm-done.json", {
        "meeting_id":       mid,
        "status":           "llm_processed",
        "processed_at":     now_utc.isoformat(),
        "processed_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "model":            model_str,
        "provider":         provider,
        "strategy":         "map-reduce" if total_input > SINGLE_CALL_INPUT_LIMIT else "single-call",
        "llm_txt":          f"s3://{S3_BUCKET}/{llm_key}",
        "doc_source":       f"s3://{S3_BUCKET}/{base_prefix}/docs/doc.txt",
        "doc_chars":        len(doc_txt),
        "transcript_chars": len(transcript_text),
        "input_chars":      total_input,
        "transcript":       "found" if transcript_text else "N/A",
        "base_prefix":      base_prefix,
        "raw_prefix":       raw_prefix,
        "temp_prefix":      pfx,
        "output_chars":     len(llm_output),
    })
    log.info(f"[{mid}] ✅ llm-done.json written")

    input_tokens  = total_input // 4
    output_tokens = len(llm_output) // 4
    if provider == "bedrock":
        est_cost = (input_tokens / 1_000_000 * 0.80) + (output_tokens / 1_000_000 * 4.00)
    else:
        est_cost = (input_tokens / 1_000_000 * 0.15) + (output_tokens / 1_000_000 * 0.60)

    log.info(
        f"[{mid}] 💰 ~{input_tokens:,} in / ~{output_tokens:,} out "
        f"/ est ~${est_cost:.4f} / provider={provider}"
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
    log.info("llm_processor_worker starting (prompt v3.1 — single-call + map-reduce)")
    log.info(f"  Primary model:           {BEDROCK_MODEL_ID}")
    log.info(f"  Fallback model:          {OPENAI_MODEL_ID}")
    log.info(f"  Workers:                 {BACKFILL_WORKERS}")
    log.info(f"  Bedrock max output:      {BEDROCK_MAX_OUTPUT_TOKENS} tokens")
    log.info(f"  OpenAI max output:       {OPENAI_MAX_OUTPUT_TOKENS} tokens")
    log.info(f"  Single-call input limit: {SINGLE_CALL_INPUT_LIMIT:,} chars")
    log.info(f"  Map-reduce chunk size:   {MAP_REDUCE_CHUNK_SIZE:,} chars")
    log.info(f"  Transcript retries:      {MAX_TRANSCRIPT_RETRIES} × {RETRY_WAIT_MINUTES}min")
    log.info(f"  Scan interval:           {SCAN_INTERVAL}s")
    log.info(f"  Path resolution:         auto (new + old + S3 scan fallback)")
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
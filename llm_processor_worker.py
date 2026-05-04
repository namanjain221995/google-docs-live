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
  3. Check transcript exists in S3 — wait up to 60min with 6 retries
  4. Read doc.txt + TRANSCRIPT/*.vtt
  5. Call Claude Haiku 4.5 via Bedrock (3-part split + smart skip + caching)
  6. Save llm.txt to final S3 path
  7. Write llm-done.json to temp

done.json formats supported:
  Format A (old finalizer):  "final_s3_prefix": "Interview-Success/..."
  Format B (new finalizer):  "final_s3_prefix" + "final_doc_txt" + "temp_prefix"
  Format C (llm format):     "base_prefix": "Interview-Success/..."
  Format D (fallback):       extract from "final_doc_txt" by stripping /docs/doc.txt

Transcript wait logic:
  - Checks TRANSCRIPT/*.vtt before running LLM
  - No transcript → write llm-retry.json, skip and retry in 10 min
  - After 6 retries (60 min) → write llm-error.json, skip forever
  - Scanner skips meetings with llm-done.json OR llm-error.json

S3 temp files:
  state.json      — created by meeting_start_worker
  doc.txt         — created by google_change_worker
  done.json       — created by doc_finalizer_worker  ← trigger
  llm-retry.json  — retry counter (transcript wait)
  llm-done.json   — LLM success marker
  llm-error.json  — permanent failure (transcript never arrived)
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
OPENAI_MODEL_ID   = "gpt-4o-mini"       # fallback when Bedrock throttles
MAX_OUTPUT_TOKENS = 8192
BACKFILL_WORKERS  = 30
SCAN_INTERVAL     = 120        # seconds between backfill scans
IST_OFFSET        = timedelta(hours=5, minutes=30)

# Throttle detection
THROTTLE_ERRORS   = ("ThrottlingException", "Too many tokens", "rate limit", "quota")

# Transcript wait config
MAX_TRANSCRIPT_RETRIES = 6     # max retries waiting for transcript
RETRY_WAIT_MINUTES     = 10    # minutes between each retry
# Total max wait = 6 × 10 = 60 minutes before giving up

# Smart skip: if Part 1 has this many final sections, skip Parts 2+3
SMART_SKIP_THRESHOLD = 7

# ── LOGGING ───────────────────────────────────────────────────────────────────
os.makedirs("/home/ec2-user/google-docs-live/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            "/home/ec2-user/google-docs-live/logs/llm_processor_worker.log"
        ),
    ],
)
log = logging.getLogger("llm_processor_worker")

# ── AWS CLIENTS ───────────────────────────────────────────────────────────────
_boto_cfg = Config(
    max_pool_connections=150,
    read_timeout=1200,
    connect_timeout=30,
)
s3c     = boto3.client("s3",              region_name=AWS_REGION, config=_boto_cfg)
sm      = boto3.client("secretsmanager",  region_name=AWS_REGION)
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION, config=_boto_cfg)

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
    """Write dict as JSON to S3."""
    s3c.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

def s3_put_text(key, text):
    """Write plain text to S3."""
    s3c.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain",
    )

# ── BASE PREFIX EXTRACTOR ─────────────────────────────────────────────────────

def extract_base_prefix(done: dict) -> tuple:
    """
    Extract base_prefix from done.json.
    Handles ALL known formats:

      Format A (old finalizer, pre-May 2026):
        "final_s3_prefix": "Interview-Success/Host/..."

      Format B (new finalizer, May 2026+):
        "final_s3_prefix": "Interview-Success/Host/..."
        "final_doc_txt":   "s3://bucket/Interview-Success/.../docs/doc.txt"
        "temp_prefix":     "temp/live-doc-history/..."

      Format C (llm_processor_worker written):
        "base_prefix": "Interview-Success/Host/..."

      Format D (only final_doc_txt, no prefix field):
        "final_doc_txt": "s3://bucket/.../docs/doc.txt"
        → strip /docs/doc.txt to get base_prefix

    Returns (base_prefix, key_used) or ("", "none")
    """
    def clean(raw):
        return raw.replace(f"s3://{S3_BUCKET}/", "").rstrip("/").strip()

    # 1. base_prefix (new llm format)
    bp = clean(done.get("base_prefix", ""))
    if bp:
        return bp, "base_prefix"

    # 2. final_s3_prefix (doc_finalizer format — most common)
    bp = clean(done.get("final_s3_prefix", ""))
    if bp:
        return bp, "final_s3_prefix"

    # 3. Extract from final_doc_txt by stripping /docs/doc.txt
    doc_txt_url = done.get("final_doc_txt", "")
    if doc_txt_url:
        bp = clean(doc_txt_url)
        if bp.endswith("/docs/doc.txt"):
            bp = bp[: -len("/docs/doc.txt")]
        if bp:
            return bp, "final_doc_txt"

    return "", "none"

# ══════════════════════════════════════════════════════════════════════════════
# TRANSCRIPT WAIT LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def has_transcript(base_prefix: str) -> bool:
    """Return True if TRANSCRIPT/*.vtt exists under base_prefix in S3."""
    try:
        resp = s3c.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{base_prefix}/TRANSCRIPT/",
            MaxKeys=10,
        )
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".vtt"):
                log.info(f"Transcript found: {obj['Key']}")
                return True
    except Exception as e:
        log.warning(f"Transcript check error [{base_prefix}]: {e}")
    return False


def read_retry_state(temp_prefix: str) -> dict:
    """Read llm-retry.json. Returns {} if not exists."""
    raw = s3_read(f"{temp_prefix}/llm-retry.json")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def write_retry_state(temp_prefix: str, meeting_id: str, retries: int,
                      first_checked_at: str, base_prefix: str):
    """Write/update llm-retry.json in S3 temp."""
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
    """Write llm-error.json — permanent failure, scanner skips forever."""
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
    """True if >= RETRY_WAIT_MINUTES have passed since last check."""
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
    """
    Check transcript and manage retry logic.

    Returns:
      "go"   — transcript found, proceed with LLM
      "wait" — no transcript yet, saved retry state, skip this cycle
      "stop" — retries exhausted, llm-error.json written, skip forever
    """
    # Transcript exists → proceed
    if has_transcript(base_prefix):
        log.info(f"[{meeting_id}] ✅ Transcript found — proceeding")
        return "go"

    # No transcript — read retry state
    state   = read_retry_state(temp_prefix)
    retries = state.get("retries", 0)
    now_utc = datetime.now(timezone.utc)

    # Too soon since last check
    if not should_retry_now(state):
        log.info(
            f"[{meeting_id}] ⏳ No transcript — too soon to retry "
            f"(retry {retries}/{MAX_TRANSCRIPT_RETRIES}, "
            f"last: {state.get('last_checked_at_ist', '?')})"
        )
        return "wait"

    # Count this as a retry attempt
    retries += 1

    # Retries exhausted → permanent failure
    if retries >= MAX_TRANSCRIPT_RETRIES:
        write_llm_error(temp_prefix, meeting_id, base_prefix, retries)
        return "stop"

    # Save retry state and skip this cycle
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
    """
    Scan temp/live-doc-history/ for meetings that have:
      done.json      ✅
      llm-done.json  ❌  (not yet processed)
      llm-error.json ❌  (not permanently failed)

    Handles both path structures:
      NEW: temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/<meeting_id>/file
      OLD: temp/live-doc-history/<meeting_id>/file
    """
    paginator      = s3c.get_paginator("list_objects_v2")
    has_done       = {}    # meeting_id → {pfx, lm}
    has_llm_done   = set()
    has_llm_error  = set()

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

    # Meetings needing processing
    pending = [
        {"mid": mid, "pfx": info["pfx"], "lm": info["lm"]}
        for mid, info in has_done.items()
        if mid not in has_llm_done    # not already processed
        and mid not in has_llm_error  # not permanently failed
    ]

    # Newest first
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
# TRANSCRIPT READER
# ══════════════════════════════════════════════════════════════════════════════

def parse_vtt(vtt_text: str) -> str:
    """Convert VTT transcript to plain readable text."""
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


def get_transcript_text(base_prefix: str) -> str:
    """
    Find TRANSCRIPT/*.vtt under base_prefix and return plain text.
    Returns empty string if not found.
    """
    try:
        resp = s3c.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{base_prefix}/TRANSCRIPT/",
            MaxKeys=10,
        )
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".vtt"):
                vtt = s3_read(obj["Key"])
                if vtt:
                    return parse_vtt(vtt)
    except Exception as e:
        log.warning(f"VTT read error [{base_prefix}]: {e}")
    return ""

# ══════════════════════════════════════════════════════════════════════════════
# LLM CALLS — BEDROCK PRIMARY + OPENAI FALLBACK
# ══════════════════════════════════════════════════════════════════════════════

# OpenAI API key cache
_openai_key      = None
_openai_key_lock = threading.Lock()

def get_openai_key() -> str:
    """Get OpenAI API key from AWS Secrets Manager (secrets/api)."""
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
    """Return True if exception is a Bedrock throttling/quota error."""
    msg = str(e)
    return any(t.lower() in msg.lower() for t in THROTTLE_ERRORS)


def call_bedrock_streaming(user_message: str, system_prompt: str) -> str:
    """
    Call Claude Haiku 4.5 via Bedrock with streaming + prompt caching.
    Returns full response text.
    Raises exception on failure (caller handles throttle fallback).
    """
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


def call_openai(user_message: str, system_prompt: str) -> str:
    """
    Call OpenAI GPT-4o-mini as fallback when Bedrock is throttled.
    Uses urllib (no extra library needed).
    Returns full response text.
    """
    api_key = get_openai_key()
    payload = json.dumps({
        "model":      OPENAI_MODEL_ID,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_message},
        ],
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


def call_llm(user_message: str, system_prompt: str,
             meeting_id: str = "", part: str = "") -> tuple:
    """
    Call LLM with automatic fallback:
      1. Try Bedrock (Claude Haiku 4.5)
      2. On ThrottlingException → fall back to OpenAI (GPT-4o-mini)

    Returns (response_text, provider_used)
    """
    # ── Try Bedrock first ─────────────────────────────────────────────────
    try:
        result = call_bedrock_streaming(user_message, system_prompt)
        return result, "bedrock"
    except Exception as e:
        if is_throttle_error(e):
            log.warning(
                f"[{meeting_id}] Bedrock throttled{' ('+part+')' if part else ''} "
                f"— falling back to OpenAI GPT-4o-mini"
            )
        else:
            # Non-throttle Bedrock error → re-raise, don't waste OpenAI quota
            raise

    # ── Fallback to OpenAI ────────────────────────────────────────────────
    try:
        result = call_openai(user_message, system_prompt)
        log.info(
            f"[{meeting_id}] OpenAI fallback success{' ('+part+')' if part else ''} ✅"
        )
        return result, "openai"
    except Exception as e:
        log.error(f"[{meeting_id}] OpenAI fallback also failed: {e}")
        raise


def run_llm_openai(doc_text: str, transcript_text: str,
                   meeting_id: str) -> tuple:
    """
    Single-call OpenAI analysis.
    GPT-4o-mini supports 16K output tokens so the full analysis
    fits in ONE call — no 3-part split needed.
    Returns (output_text, "openai")
    """
    prompt     = get_prompt()
    input_text = (
        f"DOCUMENT:\n{doc_text}\n\n"
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
    result = call_openai(
        f"{prompt}\n\n"
        f"Generate the COMPLETE analysis with ALL sections in one response.\n\n"
        f"{input_text}",
        prompt,
    )
    log.info(f"[{meeting_id}] OpenAI single-call done ({len(result)} chars)")
    return header + result, "openai"


def run_llm_bedrock(doc_text: str, transcript_text: str,
                    meeting_id: str) -> tuple:
    """
    3-part split for Bedrock Claude Haiku 4.5.
    Needed because Haiku has 8K output limit per call.

    Part 1: audit_metadata, audit_summary_card, role_identification,
            flat_summary, overall_dynamics_of_interview, Q&A mapping
    Part 2: candidate/proxy/interviewer performance + hiring prediction
    Part 3: improvement plan, mistakes, verdict, gaps

    Smart skip: if Part 1 has >= 7/8 final sections → skip Parts 2+3
    Returns (output_text, "bedrock")
    """
    prompt     = get_prompt()
    input_text = (
        f"DOCUMENT:\n{doc_text}\n\n"
        f"TRANSCRIPT:\n{transcript_text if transcript_text else 'N/A'}"
    )
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

    # Part 1
    log.info(f"[{meeting_id}] Bedrock Part 1 starting...")
    part1 = call_bedrock_streaming(
        f"{prompt}\n\n"
        f"OUTPUT PART 1: Generate these sections only:\n"
        f"audit_metadata, audit_summary_card, role_identification, "
        f"flat_summary, overall_dynamics_of_interview, "
        f"question_answer_support_mapping\n\n"
        f"{input_text}",
        prompt,
    )
    log.info(f"[{meeting_id}] Bedrock Part 1 done ({len(part1)} chars)")

    found = sum(1 for s in final_sections if s in part1)
    if found >= SMART_SKIP_THRESHOLD:
        log.info(f"[{meeting_id}] Smart skip: {found}/8 in Part 1 ✅")
        return header + part1, "bedrock"

    # Part 2
    log.info(f"[{meeting_id}] Bedrock Part 2 starting...")
    part2 = call_bedrock_streaming(
        f"{prompt}\n\n"
        f"CONTINUATION — Part 2.\n"
        f"Previous output (last 3000 chars):\n{part1[-3000:]}\n\n"
        f"OUTPUT PART 2: Generate ONLY missing sections:\n"
        f"overall_candidate_performance, overall_proxy_support_performance, "
        f"overall_interviewer_performance, chance_of_moving_forward\n"
        f"DO NOT regenerate any section already in Part 1.\n\n"
        f"{input_text}",
        prompt,
    )
    log.info(f"[{meeting_id}] Bedrock Part 2 done ({len(part2)} chars)")

    combined = part1 + "\n" + part2
    found2   = sum(1 for s in final_sections if s in combined)
    if found2 >= SMART_SKIP_THRESHOLD:
        log.info(f"[{meeting_id}] Smart skip after Part 2: {found2}/8 ✅")
        return header + combined, "bedrock"

    # Part 3
    log.info(f"[{meeting_id}] Bedrock Part 3 starting...")
    part3 = call_bedrock_streaming(
        f"{prompt}\n\n"
        f"CONTINUATION — Part 3 (final).\n"
        f"Previous output (last 2000 chars):\n{combined[-2000:]}\n\n"
        f"OUTPUT PART 3: Generate ONLY missing sections:\n"
        f"if_chances_are_less_next_time_plan, mistake_and_risk_ledger, "
        f"final_verdict, insufficient_context\n"
        f"DO NOT regenerate any section already written. "
        f"STOP after insufficient_context.\n\n"
        f"{input_text}",
        prompt,
    )
    log.info(f"[{meeting_id}] Bedrock Part 3 done ({len(part3)} chars)")
    return header + combined + "\n" + part3, "bedrock"


def run_llm_3part(doc_text: str, transcript_text: str,
                  meeting_id: str) -> tuple:
    """
    Master LLM runner:
      - Try Bedrock first (3-part split, Claude Haiku 4.5)
      - On ThrottlingException → fall back to OpenAI (single call, GPT-4o-mini)

    Returns (output_text, provider) where provider = "bedrock" or "openai"
    """
    # ── Try Bedrock (3-part) ──────────────────────────────────────────────
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

    # ── Fallback: OpenAI single call ─────────────────────────────────────
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

    # ── Step 2: Extract base_prefix (handles ALL done.json formats) ───────────
    base_prefix, key_used = extract_base_prefix(done)

    if not base_prefix:
        return f"SKIP {mid} — no path found in done.json (keys: {list(done.keys())})"

    log.info(f"[{mid}] base_prefix='{base_prefix}' (from '{key_used}')")

    # ── Step 3: Transcript wait check ─────────────────────────────────────────
    t_status = transcript_wait_check(mid, pfx, base_prefix)

    if t_status == "wait":
        return f"WAIT {mid} — transcript not ready, retry in ~{RETRY_WAIT_MINUTES}min"

    if t_status == "stop":
        return (f"ERROR {mid} — transcript never arrived after "
                f"{MAX_TRANSCRIPT_RETRIES} retries "
                f"({MAX_TRANSCRIPT_RETRIES * RETRY_WAIT_MINUTES} min)")

    # t_status == "go" → transcript found, proceed

    # ── Step 4: Read doc.txt ──────────────────────────────────────────────────
    doc_txt = s3_read(f"{base_prefix}/docs/doc.txt")
    if not doc_txt:
        return f"SKIP {mid} — doc.txt not found at {base_prefix}/docs/doc.txt"

    log.info(f"[{mid}] doc.txt: {len(doc_txt)} chars")

    # ── Step 5: Read transcript text ──────────────────────────────────────────
    transcript_text = get_transcript_text(base_prefix)
    log.info(f"[{mid}] Transcript: {len(transcript_text)} chars"
             f"{' (empty — will use N/A)' if not transcript_text else ''}")

    # ── Step 6: Run LLM ───────────────────────────────────────────────────────
    log.info(f"[{mid}] Starting LLM analysis (3-part + smart skip)...")
    try:
        llm_output, provider = run_llm_3part(doc_txt, transcript_text, mid)
    except Exception as e:
        log.error(f"[{mid}] LLM call failed: {e}", exc_info=True)
        return f"ERROR {mid} — LLM failed: {e}"

    if not llm_output or len(llm_output) < 100:
        return f"ERROR {mid} — LLM returned empty/short output ({len(llm_output)} chars)"

    log.info(f"[{mid}] LLM output: {len(llm_output)} chars")

    # ── Step 7: Save llm.txt to final S3 path ─────────────────────────────────
    llm_key = f"{base_prefix}/llm/llm.txt"
    s3_put_text(llm_key, llm_output)
    log.info(f"[{mid}] ✅ llm.txt saved → {llm_key}")

    # ── Step 8: Write llm-done.json to temp ───────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    s3_put_json(f"{pfx}/llm-done.json", {
        "meeting_id":       mid,
        "status":           "llm_processed",
        "processed_at":     now_utc.isoformat(),
        "processed_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "model":            f"claude-haiku-4-5 (Bedrock)" if provider == "bedrock" else f"gpt-4o-mini (OpenAI)" if provider == "openai" else f"mixed ({provider})",
        "llm_txt":          f"s3://{S3_BUCKET}/{llm_key}",
        "doc_source":       f"s3://{S3_BUCKET}/{base_prefix}/docs/doc.txt",
        "transcript":       "found" if transcript_text else "N/A",
        "base_prefix":      base_prefix,
        "temp_prefix":      pfx,
        "output_chars":     len(llm_output),
    })
    log.info(f"[{mid}] ✅ llm-done.json written")

    # ── Step 9: Cost estimate ─────────────────────────────────────────────────
    input_tokens  = (len(doc_txt) + len(transcript_text)) // 4
    output_tokens = len(llm_output) // 4
    est_cost      = (
        (input_tokens  / 1_000_000 * 0.80) +
        (output_tokens / 1_000_000 * 4.00)
    )
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

            log.info(f"Processing {len(pending)} meetings "
                     f"with {BACKFILL_WORKERS} workers...")

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
                            log.info(f"⏳ {result}")
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
    log.info(f"  Model:              {BEDROCK_MODEL_ID}")
    log.info(f"  Workers:            {BACKFILL_WORKERS}")
    log.info(f"  Max output tokens:  {MAX_OUTPUT_TOKENS}")
    log.info(f"  Smart skip at:      {SMART_SKIP_THRESHOLD}/8 sections")
    log.info(f"  Transcript retries: {MAX_TRANSCRIPT_RETRIES} × {RETRY_WAIT_MINUTES}min = "
             f"{MAX_TRANSCRIPT_RETRIES * RETRY_WAIT_MINUTES}min max wait")
    log.info(f"  Scan interval:      {SCAN_INTERVAL}s")
    log.info("=" * 60)

    # Verify prompt file
    try:
        p = get_prompt()
        log.info(f"Prompt loaded: {len(p):,} chars ✅")
    except Exception as e:
        log.error(f"Prompt file failed: {e}")
        sys.exit(1)

    # Verify Bedrock connectivity
    try:
        bedrock.list_foundation_models(byProvider="anthropic")
        log.info("Bedrock access ✅")
    except Exception as e:
        log.warning(f"Bedrock list check failed (may still work): {e}")

    # Start processing
    backfill_loop()


if __name__ == "__main__":
    main()
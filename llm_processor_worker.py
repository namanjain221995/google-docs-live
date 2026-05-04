"""
llm_processor_worker.py
=======================
LLM Analysis Worker — AWS Bedrock Claude Haiku 4.5

NEW: Transcript wait logic
  - Before running LLM, checks if TRANSCRIPT/*.vtt exists in S3
  - If no transcript: writes llm-retry.json, skips this cycle
  - Retries every ~10 minutes, max 6 retries (~60 min total)
  - After 6 retries with no transcript: writes llm-error.json, skips forever
  - Scanner also skips meetings with llm-error.json

S3 temp files:
  state.json       — created by meeting_start_worker
  doc.txt          — created by google_change_worker
  done.json        — created by doc_finalizer_worker (trigger)
  llm-retry.json   — NEW: retry counter + timestamps
  llm-done.json    — created on LLM success
  llm-error.json   — NEW: permanent failure (transcript never arrived)
"""

import os
import sys
import json
import time
import logging
import threading
import boto3
import base64
from botocore.config import Config
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────────────────────────
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET    = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
SF_SECRET    = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
API_SECRET   = os.environ.get("API_SECRET_NAME", "secrets/api")
PROMPT_FILE  = os.environ.get("PROMPT_FILE",
               "/home/ec2-user/google-docs-live/prompt.txt")

BEDROCK_MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
MAX_OUTPUT_TOKENS = 8192
BACKFILL_WORKERS  = 30
SCAN_INTERVAL     = 120   # seconds between backfill scans
IST_OFFSET        = timedelta(hours=5, minutes=30)

# ── TRANSCRIPT RETRY CONFIG ───────────────────────────────────────────────────
MAX_TRANSCRIPT_RETRIES  = 6     # max attempts waiting for transcript
RETRY_WAIT_MINUTES      = 10    # minutes between retries
# Total max wait = 6 × 10 = 60 minutes

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
s3c      = boto3.client("s3",             region_name=AWS_REGION, config=_boto_cfg)
sm       = boto3.client("secretsmanager", region_name=AWS_REGION)
bedrock  = boto3.client("bedrock-runtime", region_name=AWS_REGION, config=_boto_cfg)

# ── SECRETS ───────────────────────────────────────────────────────────────────
_sec_cache = {}
_sec_lock  = threading.Lock()

def get_secret(name):
    with _sec_lock:
        if name not in _sec_cache:
            raw = sm.get_secret_value(SecretId=name)["SecretString"]
            _sec_cache[name] = json.loads(raw)
        return _sec_cache[name]

# ── PROMPT ────────────────────────────────────────────────────────────────────
_prompt = None
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
        Body=json.dumps(data, indent=2).encode(),
        ContentType="application/json",
    )

def s3_exists(key):
    try:
        s3c.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════════════════════
# TRANSCRIPT WAIT LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def has_transcript(base_prefix):
    """Check if TRANSCRIPT/*.vtt exists under final S3 path."""
    prefix = f"{base_prefix}/TRANSCRIPT/"
    try:
        resp = s3c.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=10
        )
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".vtt"):
                log.info(f"Transcript found: {obj['Key']}")
                return True
        return False
    except Exception as e:
        log.warning(f"Transcript check error [{base_prefix}]: {e}")
        return False


def read_retry_state(temp_prefix):
    """Read llm-retry.json. Returns {} if not exists."""
    raw = s3_read(f"{temp_prefix}/llm-retry.json")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def write_retry_state(temp_prefix, meeting_id, state):
    """Write llm-retry.json to S3 temp."""
    now_utc = datetime.now(timezone.utc)
    state.update({
        "meeting_id":     meeting_id,
        "updated_at":     now_utc.isoformat(),
        "updated_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "max_retries":    MAX_TRANSCRIPT_RETRIES,
        "retry_interval_minutes": RETRY_WAIT_MINUTES,
    })
    s3_put_json(f"{temp_prefix}/llm-retry.json", state)


def write_llm_error(temp_prefix, meeting_id, base_prefix, retries):
    """Write llm-error.json — permanent failure, scanner skips this forever."""
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
        "action":            "LLM skipped permanently — no transcript available",
    })
    log.warning(
        f"[{meeting_id}] ❌ llm-error.json written — "
        f"transcript never arrived after {retries} retries "
        f"({retries * RETRY_WAIT_MINUTES} min)"
    )


def should_retry_now(state):
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


def transcript_wait_check(meeting_id, temp_prefix, base_prefix):
    """
    Check transcript availability with retry logic.

    Returns:
      "go"   — transcript found, proceed with LLM
      "wait" — no transcript yet, will retry later
      "stop" — retries exhausted, llm-error.json written

    Retry flow:
      - Each call that finds no transcript increments retry counter
      - Only retries if >= RETRY_WAIT_MINUTES since last check
      - After MAX_TRANSCRIPT_RETRIES → permanent error
    """
    # ── Transcript found → proceed ────────────────────────────────────────
    if has_transcript(base_prefix):
        log.info(f"[{meeting_id}] ✅ Transcript found — proceeding with LLM")
        return "go"

    # ── No transcript — check retry state ────────────────────────────────
    state   = read_retry_state(temp_prefix)
    retries = state.get("retries", 0)
    now_utc = datetime.now(timezone.utc)

    # Too soon to retry (< 10 min since last check)
    if not should_retry_now(state):
        last_ist = state.get("updated_at_ist", "?")
        log.info(
            f"[{meeting_id}] ⏳ No transcript — too soon to retry "
            f"(retry {retries}/{MAX_TRANSCRIPT_RETRIES}, last check: {last_ist})"
        )
        return "wait"

    # Enough time passed — count this as a retry attempt
    retries += 1

    # Retries exhausted → permanent failure
    if retries >= MAX_TRANSCRIPT_RETRIES:
        write_llm_error(temp_prefix, meeting_id, base_prefix, retries)
        return "stop"

    # Save updated retry state and skip this cycle
    write_retry_state(temp_prefix, meeting_id, {
        "retries":          retries,
        "first_checked_at": state.get("first_checked_at", now_utc.isoformat()),
        "last_checked_at":  now_utc.isoformat(),
        "base_prefix":      base_prefix,
    })
    log.info(
        f"[{meeting_id}] ⏳ No transcript yet — "
        f"retry {retries}/{MAX_TRANSCRIPT_RETRIES} "
        f"(next check in ~{RETRY_WAIT_MINUTES} min)"
    )
    return "wait"

# ══════════════════════════════════════════════════════════════════════════════
# S3 SCANNER
# ══════════════════════════════════════════════════════════════════════════════

def scan_s3_for_unprocessed():
    """
    Find meetings with done.json but no llm-done.json and no llm-error.json.
    Handles NEW and OLD temp path structures.
    """
    paginator      = s3c.get_paginator("list_objects_v2")
    has_done       = {}   # meeting_id → {pfx, lm}
    has_llm_done   = set()
    has_llm_error  = set()  # NEW: permanent failures — skip forever

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
                has_llm_error.add(mid)  # ← NEW: permanent failure

    # Meetings ready to process
    pending = [
        {"mid": mid, "pfx": info["pfx"], "lm": info["lm"]}
        for mid, info in has_done.items()
        if mid not in has_llm_done    # not already processed
        and mid not in has_llm_error  # not permanently failed  ← NEW
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
        f"{len(has_llm_error)} permanently failed (no transcript)"
    )
    return pending

# ══════════════════════════════════════════════════════════════════════════════
# VTT PARSER
# ══════════════════════════════════════════════════════════════════════════════

def parse_vtt(vtt_text):
    """Convert VTT transcript to plain text."""
    lines = []
    for line in vtt_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("WEBVTT") or "-->" in line:
            continue
        if line.isdigit():
            continue
        lines.append(line)
    return " ".join(lines)


def get_transcript_text(base_prefix):
    """
    Find and read TRANSCRIPT/*.vtt from S3.
    Returns plain text or empty string.
    """
    prefix = f"{base_prefix}/TRANSCRIPT/"
    try:
        resp = s3c.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=10)
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".vtt"):
                vtt = s3_read(obj["Key"])
                if vtt:
                    return parse_vtt(vtt)
    except Exception as e:
        log.warning(f"VTT read error [{base_prefix}]: {e}")
    return ""

# ══════════════════════════════════════════════════════════════════════════════
# LLM CALL (Bedrock Streaming)
# ══════════════════════════════════════════════════════════════════════════════

def call_bedrock_streaming(prompt_text, system_prompt):
    """
    Call Claude Haiku 4.5 via Bedrock with streaming + prompt caching.
    Returns full response text.
    """
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": MAX_OUTPUT_TOKENS,
        "system": [
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        "messages": [
            {"role": "user", "content": prompt_text}
        ],
    })

    response = bedrock.invoke_model_with_response_stream(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=body,
    )

    full_text = []
    stream = response.get("body")
    if stream:
        for event in stream:
            chunk = event.get("chunk")
            if chunk:
                chunk_data = json.loads(chunk.get("bytes", b"{}"))
                if chunk_data.get("type") == "content_block_delta":
                    delta = chunk_data.get("delta", {})
                    if delta.get("type") == "text_delta":
                        full_text.append(delta.get("text", ""))

    return "".join(full_text)


def run_llm_3part(doc_text, transcript_text, meeting_id):
    """
    3-part split with smart skip.
    Part 1: META + SUMMARY + ROLES + FLOW + DYNAMICS + Q&A
    Part 2: CANDIDATE + SUPPORT + INTERVIEWER + HIRING (if missing)
    Part 3: IMPROVEMENT + MISTAKES + VERDICT + GAPS (if missing)
    Smart skip: if Part 1 has 7+ final sections → skip Parts 2 & 3
    """
    prompt      = get_prompt()
    input_text  = f"DOCUMENT:\n{doc_text}\n\nTRANSCRIPT:\n{transcript_text or 'N/A'}"

    now_utc = datetime.now(timezone.utc)
    header  = (
        f"LLM ANALYSIS REPORT\n"
        f"Meeting ID: {meeting_id}\n"
        f"Generated At: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC "
        f"({(now_utc + IST_OFFSET).strftime('%Y-%m-%d %I:%M:%S %p')} IST)\n"
        f"Model: Claude Haiku 4.5 (AWS Bedrock)\n"
        f"\n{'='*60}\n"
    )

    # Part 1
    log.info(f"[{meeting_id}] LLM Part 1 starting...")
    part1_prompt = (
        f"{prompt}\n\n"
        f"OUTPUT PART 1: Generate sections: "
        f"audit_metadata, audit_summary_card, role_identification, "
        f"flat_summary, overall_dynamics_of_interview, question_answer_support_mapping\n\n"
        f"{input_text}"
    )
    part1 = call_bedrock_streaming(part1_prompt, prompt)
    log.info(f"[{meeting_id}] LLM Part 1 done ({len(part1)} chars)")

    # Smart skip: count final sections in part1
    final_sections = [
        "overall_candidate_performance", "overall_proxy_support_performance",
        "overall_interviewer_performance", "chance_of_moving_forward",
        "if_chances_are_less_next_time_plan", "mistake_and_risk_ledger",
        "final_verdict", "insufficient_context",
    ]
    found = sum(1 for s in final_sections if s in part1)
    if found >= 7:
        log.info(f"[{meeting_id}] Smart skip: {found}/8 final sections in Part 1 ✅")
        return header + part1

    # Part 2
    log.info(f"[{meeting_id}] LLM Part 2 starting...")
    part2_prompt = (
        f"{prompt}\n\n"
        f"CONTINUATION — Part 2. Previous output:\n{part1[:3000]}...\n\n"
        f"OUTPUT PART 2: Generate ONLY missing sections: "
        f"overall_candidate_performance, overall_proxy_support_performance, "
        f"overall_interviewer_performance, chance_of_moving_forward\n"
        f"Start immediately with the first missing section. "
        f"DO NOT regenerate sections already in Part 1.\n\n"
        f"{input_text}"
    )
    part2 = call_bedrock_streaming(part2_prompt, prompt)
    log.info(f"[{meeting_id}] LLM Part 2 done ({len(part2)} chars)")

    combined = part1 + "\n" + part2
    found2 = sum(1 for s in final_sections if s in combined)
    if found2 >= 7:
        log.info(f"[{meeting_id}] Smart skip after Part 2: {found2}/8 sections ✅")
        return header + combined

    # Part 3
    log.info(f"[{meeting_id}] LLM Part 3 starting...")
    part3_prompt = (
        f"{prompt}\n\n"
        f"CONTINUATION — Part 3 (final). Previous output summary:\n"
        f"{combined[-2000:]}...\n\n"
        f"OUTPUT PART 3: Generate ONLY missing sections: "
        f"if_chances_are_less_next_time_plan, mistake_and_risk_ledger, "
        f"final_verdict, insufficient_context\n"
        f"Start immediately with the first missing section. "
        f"DO NOT regenerate any sections already written. "
        f"STOP IMMEDIATELY after insufficient_context.\n\n"
        f"{input_text}"
    )
    part3 = call_bedrock_streaming(part3_prompt, prompt)
    log.info(f"[{meeting_id}] LLM Part 3 done ({len(part3)} chars)")

    return header + combined + "\n" + part3

# ══════════════════════════════════════════════════════════════════════════════
# CORE: PROCESS ONE MEETING
# ══════════════════════════════════════════════════════════════════════════════

def process_one_meeting(item):
    mid = item["mid"]
    pfx = item["pfx"]
    log.info(f"[{mid}] ── Processing ──")

    # ── 1. Read done.json → get base_prefix ──────────────────────────────
    done_raw = s3_read(f"{pfx}/done.json")
    if not done_raw:
        return f"SKIP {mid} — done.json missing"
    try:
        done = json.loads(done_raw)
    except Exception:
        return f"SKIP {mid} — done.json parse error"

    base_prefix = done.get("base_prefix", "").replace(
        f"s3://{S3_BUCKET}/", "").rstrip("/")
    if not base_prefix:
        return f"SKIP {mid} — base_prefix empty in done.json"

    log.info(f"[{mid}] base_prefix='{base_prefix}'")

    # ── 2. Transcript wait check (NEW) ───────────────────────────────────
    transcript_status = transcript_wait_check(mid, pfx, base_prefix)

    if transcript_status == "wait":
        return f"WAIT {mid} — transcript not ready, retry in ~{RETRY_WAIT_MINUTES}min"

    if transcript_status == "stop":
        return f"ERROR {mid} — transcript never arrived after {MAX_TRANSCRIPT_RETRIES} retries"

    # transcript_status == "go" → proceed with LLM

    # ── 3. Read doc.txt ───────────────────────────────────────────────────
    doc_txt = s3_read(f"{base_prefix}/docs/doc.txt")
    if not doc_txt:
        return f"SKIP {mid} — doc.txt not found"

    # ── 4. Read transcript ────────────────────────────────────────────────
    transcript_text = get_transcript_text(base_prefix)
    log.info(f"[{mid}] Transcript length: {len(transcript_text)} chars")

    # ── 5. Run LLM ────────────────────────────────────────────────────────
    log.info(f"[{mid}] Starting LLM analysis...")
    try:
        llm_output = run_llm_3part(doc_txt, transcript_text, mid)
    except Exception as e:
        log.error(f"[{mid}] LLM call failed: {e}", exc_info=True)
        return f"ERROR {mid} — LLM call failed: {e}"

    log.info(f"[{mid}] LLM output: {len(llm_output)} chars")

    # ── 6. Save llm.txt to final S3 path ─────────────────────────────────
    llm_key = f"{base_prefix}/llm/llm.txt"
    s3c.put_object(
        Bucket=S3_BUCKET, Key=llm_key,
        Body=llm_output.encode("utf-8"),
        ContentType="text/plain",
    )
    log.info(f"[{mid}] ✅ llm.txt saved: {llm_key}")

    # ── 7. Write llm-done.json to temp ───────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    s3_put_json(f"{pfx}/llm-done.json", {
        "meeting_id":      mid,
        "status":          "llm_processed",
        "processed_at":    now_utc.isoformat(),
        "processed_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "model":           "claude-haiku-4-5 (AWS Bedrock)",
        "llm_txt":         f"s3://{S3_BUCKET}/{llm_key}",
        "doc_source":      f"s3://{S3_BUCKET}/{base_prefix}/docs/doc.txt",
        "transcript":      "found" if transcript_text else "N/A",
        "base_prefix":     base_prefix,
        "temp_prefix":     pfx,
        "output_chars":    len(llm_output),
    })
    log.info(f"[{mid}] ✅ llm-done.json written")

    # ── 8. Cost estimate log ──────────────────────────────────────────────
    input_tokens  = (len(doc_txt) + len(transcript_text)) // 4
    output_tokens = len(llm_output) // 4
    est_cost      = (input_tokens / 1_000_000 * 0.80) + (output_tokens / 1_000_000 * 4.00)
    log.info(
        f"[{mid}] 💰 ~{input_tokens:,} input tokens, "
        f"~{output_tokens:,} output tokens, "
        f"est cost ~${est_cost:.4f}"
    )

    return f"OK {mid} | {len(llm_output)} chars | ${est_cost:.4f}"

# ══════════════════════════════════════════════════════════════════════════════
# BACKFILL LOOP
# ══════════════════════════════════════════════════════════════════════════════

def backfill_loop():
    log.info("Backfill loop started")
    while True:
        try:
            pending = scan_s3_for_unprocessed()

            if not pending:
                log.info(f"Backfill: nothing to process. Sleeping {SCAN_INTERVAL}s")
                time.sleep(SCAN_INTERVAL)
                continue

            log.info(f"Backfill: processing {len(pending)} meetings with {BACKFILL_WORKERS} workers")
            with ThreadPoolExecutor(
                max_workers=BACKFILL_WORKERS,
                thread_name_prefix="llm-worker"
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

            log.info(f"Backfill batch done. Sleeping {SCAN_INTERVAL}s")
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
    log.info(f"  Model:          {BEDROCK_MODEL_ID}")
    log.info(f"  Workers:        {BACKFILL_WORKERS}")
    log.info(f"  Max retries:    {MAX_TRANSCRIPT_RETRIES} (transcript wait)")
    log.info(f"  Retry interval: {RETRY_WAIT_MINUTES} min")
    log.info(f"  Total wait max: {MAX_TRANSCRIPT_RETRIES * RETRY_WAIT_MINUTES} min")
    log.info("=" * 60)

    # Verify prompt file
    try:
        p = get_prompt()
        log.info(f"Prompt loaded: {len(p)} chars ✅")
    except Exception as e:
        log.error(f"Prompt file error: {e}")
        sys.exit(1)

    # Verify Bedrock access
    try:
        bedrock.list_foundation_models(byProvider="anthropic")
        log.info("Bedrock access ✅")
    except Exception as e:
        log.warning(f"Bedrock list check failed (may still work): {e}")

    backfill_loop()


if __name__ == "__main__":
    main()
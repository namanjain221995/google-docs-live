"""
llm_processor_worker.py
-----------------------
Scans S3 for meetings that have done.json but no llm-done.json.
Uses AWS Bedrock Claude Haiku 4.5 for LLM analysis.

For each meeting:
1. Searches ALL of Interview-Success for /<meeting_id>/
2. Finds the EXACT folder that has TRANSCRIPT/*.vtt
3. Uses THAT same folder for doc.txt, transcript, llm output
4. Creates llm-done.json in temp

Uses 30 parallel workers. Newest meetings processed first.
"""

import os
import sys
import json
import time
import logging
import threading
import boto3
from botocore.config import Config
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ──
AWS_REGION      = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET       = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
PROMPT_FILE     = os.environ.get("PROMPT_FILE",
    "/home/ec2-user/google-docs-live/prompt.txt")
MAX_WORKERS     = 30
POLL_INTERVAL   = 60
IST_OFFSET      = timedelta(hours=5, minutes=30)
DEPARTMENTS     = ["Interview-Success", "Training", "Customer-Success", "Marketing"]

# Bedrock model
BEDROCK_MODEL_ID = "anthropic.claude-haiku-4-5-20251001"

# ── LOGGING ──
os.makedirs("/home/ec2-user/google-docs-live/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ec2-user/google-docs-live/logs/llm_processor_worker.log"),
    ]
)
log = logging.getLogger("llm_processor_worker")

# ── AWS ── (large connection pool for 30 workers)
boto_config = Config(max_pool_connections=150)
s3              = boto3.client("s3", region_name=AWS_REGION, config=boto_config)
bedrock_runtime = boto3.client("bedrock-runtime", region_name=AWS_REGION, config=boto_config)

# ── PROMPT ──
def load_prompt() -> str:
    try:
        with open(PROMPT_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        log.error(f"Failed to load prompt.txt: {e}")
        return ""

# ── VTT PARSER ──
def parse_vtt(vtt_content: str) -> str:
    lines    = vtt_content.split("\n")
    segments = []
    current  = {"speaker": "", "text": ""}
    for line in lines:
        line = line.strip()
        if not line or line == "WEBVTT" or line.isdigit() or "-->" in line:
            continue
        if ": " in line:
            speaker, text = line.split(": ", 1)
            speaker = speaker.strip()
            text    = text.strip()
            if speaker == current["speaker"]:
                current["text"] += " " + text
            else:
                if current["speaker"]:
                    segments.append(f"{current['speaker']}: {current['text']}")
                current = {"speaker": speaker, "text": text}
        else:
            if current["speaker"] and line:
                current["text"] += " " + line
    if current["speaker"]:
        segments.append(f"{current['speaker']}: {current['text']}")
    return "\n".join(segments)

# ── S3 HELPERS ──
def read_s3_text(key: str) -> str:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8")
    except Exception:
        return ""

def write_s3_text(key: str, content: str):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=content.encode("utf-8"), ContentType="text/plain"
    )

def write_s3_json(key: str, data: dict):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

# ── FIND REAL FOLDER FOR MEETING ──
def find_meeting_folder(meeting_id: str) -> dict | None:
    paginator  = s3.get_paginator("list_objects_v2")
    search_str = f"/{meeting_id}/"
    prefix_files = {}

    for dept in DEPARTMENTS:
        try:
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{dept}/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if search_str not in key:
                        continue
                    parts = key.split("/")
                    try:
                        mid_idx = parts.index(meeting_id)
                    except ValueError:
                        continue
                    if dept == "Interview-Success":
                        end = mid_idx + 5
                    else:
                        end = mid_idx + 3
                    if len(parts) > end:
                        base = "/".join(parts[:end])
                    else:
                        base = "/".join(parts[:mid_idx + 1])
                    if base not in prefix_files:
                        prefix_files[base] = []
                    prefix_files[base].append(key)
        except Exception as e:
            log.warning(f"Search error in {dept}: {e}")

    if not prefix_files:
        return None

    best_prefix    = None
    transcript_key = None

    for prefix, keys in prefix_files.items():
        for key in keys:
            if "/TRANSCRIPT/" in key and key.endswith(".vtt"):
                best_prefix    = prefix
                transcript_key = key
                break
        if best_prefix:
            break

    if not best_prefix:
        for prefix, keys in prefix_files.items():
            for key in keys:
                if key.endswith("/docs/doc.txt"):
                    best_prefix = prefix
                    break
            if best_prefix:
                break

    if not best_prefix:
        best_prefix = sorted(prefix_files.keys())[0]

    log.info(f"Found folder for meeting_id={meeting_id}: {best_prefix}")

    return {
        "base_prefix":    best_prefix,
        "transcript_key": transcript_key,
        "doc_key":        f"{best_prefix}/docs/doc.txt",
        "llm_key":        f"{best_prefix}/llm/llm.txt",
    }

# ── FIND UNPROCESSED MEETINGS ──
def find_unprocessed_meetings() -> list[dict]:
    paginator   = s3.get_paginator("list_objects_v2")
    has_done    = {}
    has_llmdone = set()

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key           = obj["Key"]
            last_modified = obj.get("LastModified")
            parts         = key.split("/")

            if len(parts) == 7 and parts[2].isdigit() and parts[3].startswith("Month-"):
                meeting_id = parts[5]
                filename   = parts[6]
                prefix     = "/".join(parts[:6])
            elif len(parts) == 4 and parts[2].isdigit():
                meeting_id = parts[2]
                filename   = parts[3]
                prefix     = "/".join(parts[:3])
            else:
                continue

            if filename == "done.json":
                has_done[meeting_id] = {
                    "key":           key,
                    "prefix":        prefix,
                    "last_modified": last_modified,
                }
            elif filename == "llm-done.json":
                has_llmdone.add(meeting_id)

    unprocessed = []
    for meeting_id, info in has_done.items():
        if meeting_id not in has_llmdone:
            unprocessed.append({
                "meeting_id":    meeting_id,
                "temp_prefix":   info["prefix"],
                "done_key":      info["key"],
                "last_modified": info["last_modified"],
            })

    # Sort NEWEST first
    unprocessed.sort(
        key=lambda x: x["last_modified"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )

    if unprocessed:
        newest = unprocessed[0]
        oldest = unprocessed[-1]
        log.info(
            f"Found {len(has_done)} finalized, "
            f"{len(has_llmdone)} LLM processed, "
            f"{len(unprocessed)} need processing "
            f"(newest: {newest['meeting_id']}, oldest: {oldest['meeting_id']})"
        )
    else:
        log.info(f"Found {len(has_done)} finalized, {len(has_llmdone)} LLM processed, 0 need processing")

    return unprocessed

# ── CALL BEDROCK CLAUDE HAIKU 4.5 ──
def call_bedrock(prompt: str, doc_txt: str, transcript: str) -> str:
    # Truncate to fit within context window
    doc_trunc        = doc_txt[:30000]
    transcript_trunc = transcript[:30000]

    # Build JSON input as prompt expects
    user_input = json.dumps({
        "transcript_webvtt": transcript_trunc,
        "document_version_history": doc_trunc,
        "analysis_context": {
            "candidate_name_hint": None,
            "company_name_hint": None,
            "role_title_hint": None,
            "round_type_hint": None,
            "timezone_hint": "IST",
            "meeting_start_absolute_hint": None,
            "is_mock_interview": True
        },
        "support_reference_materials": []
    }, ensure_ascii=False)

    user_message = prompt + "\n\nINPUT:\n" + user_input

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [
            {
                "role": "user",
                "content": user_message
            }
        ]
    })

    response = bedrock_runtime.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=body,
        contentType="application/json",
        accept="application/json"
    )

    response_body = json.loads(response["body"].read())
    return response_body["content"][0]["text"].strip()

# ── PROCESS ONE MEETING ──
def process_one_meeting(item: dict) -> str:
    meeting_id  = item["meeting_id"]
    temp_prefix = item["temp_prefix"]

    log.info(f"Processing meeting_id={meeting_id}")

    # Find real S3 folder
    folder = find_meeting_folder(meeting_id)
    if not folder:
        return f"SKIP  {meeting_id} — no S3 folder found"

    base_prefix    = folder["base_prefix"]
    transcript_key = folder["transcript_key"]
    doc_key        = folder["doc_key"]
    llm_key        = folder["llm_key"]

    # Read transcript
    transcript = "(No transcript available)"
    if transcript_key:
        vtt_raw = read_s3_text(transcript_key)
        if vtt_raw:
            transcript = parse_vtt(vtt_raw)
            log.info(f"Transcript loaded: {len(transcript)} chars")

    # Read doc.txt
    doc_txt = read_s3_text(doc_key)
    if not doc_txt:
        doc_txt = read_s3_text(f"{temp_prefix}/doc.txt")
    if not doc_txt:
        doc_txt = "(No interview notes available)"

    # Skip if nothing to analyze
    if doc_txt == "(No interview notes available)" and transcript == "(No transcript available)":
        return f"SKIP  {meeting_id} — no doc.txt and no transcript"

    # Load prompt
    prompt = load_prompt()
    if not prompt:
        return f"ERROR {meeting_id} — prompt.txt empty"

    # Call Bedrock with retry
    log.info(f"Calling Bedrock Claude Haiku 4.5 for meeting_id={meeting_id}...")
    llm_output = None
    for attempt in range(3):
        try:
            llm_output = call_bedrock(prompt, doc_txt, transcript)
            if llm_output:
                break
            log.warning(f"Empty output attempt {attempt+1} for {meeting_id}")
        except Exception as e:
            log.warning(f"Bedrock attempt {attempt+1} failed for {meeting_id}: {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                return f"ERROR {meeting_id} — Bedrock failed after 3 attempts: {e}"

    if not llm_output:
        return f"ERROR {meeting_id} — Bedrock returned empty output"

    # Save llm.txt
    now_utc = datetime.now(timezone.utc)
    now_ist = (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST")

    llm_content = f"""LLM ANALYSIS REPORT
Meeting ID: {meeting_id}
Generated At: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')} ({now_ist})
Model: Claude Haiku 4.5 (AWS Bedrock)
Base Folder: s3://{S3_BUCKET}/{base_prefix}
Doc Source: s3://{S3_BUCKET}/{doc_key}
Transcript: s3://{S3_BUCKET}/{transcript_key or 'N/A'}

{'='*60}
{llm_output}
"""
    write_s3_text(llm_key, llm_content)
    log.info(f"llm.txt saved: s3://{S3_BUCKET}/{llm_key}")

    # Create llm-done.json in temp
    llm_done = {
        "meeting_id":       meeting_id,
        "status":           "llm_processed",
        "processed_at":     now_utc.isoformat(),
        "processed_at_ist": now_ist,
        "model":            "claude-haiku-4-5 (AWS Bedrock)",
        "llm_txt":          f"s3://{S3_BUCKET}/{llm_key}",
        "doc_source":       f"s3://{S3_BUCKET}/{doc_key}",
        "transcript":       f"s3://{S3_BUCKET}/{transcript_key}" if transcript_key else "N/A",
        "base_prefix":      base_prefix,
        "temp_prefix":      temp_prefix,
    }
    write_s3_json(f"{temp_prefix}/llm-done.json", llm_done)
    log.info(f"llm-done.json created for meeting_id={meeting_id}")

    return f"OK    {meeting_id} → s3://{S3_BUCKET}/{llm_key}"

# ── MAIN ──
def main():
    log.info(f"llm_processor_worker starting — model=Claude Haiku 4.5 (Bedrock) — max_workers={MAX_WORKERS}")

    prompt = load_prompt()
    if not prompt:
        log.error(f"prompt.txt not found at {PROMPT_FILE}")
        sys.exit(1)
    log.info(f"Prompt loaded: {len(prompt)} chars")

    # Test Bedrock connection
    try:
        bedrock_runtime.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 10,
                "messages": [{"role": "user", "content": "hi"}]
            }),
            contentType="application/json",
            accept="application/json"
        )
        log.info("Bedrock Claude Haiku 4.5 connection verified ✅")
    except Exception as e:
        log.error(f"Bedrock connection failed: {e}")
        sys.exit(1)

    while True:
        try:
            items = find_unprocessed_meetings()

            if not items:
                log.info(f"No unprocessed meetings. Waiting {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)
                continue

            log.info(f"Processing {len(items)} meetings with {MAX_WORKERS} workers...")

            with ThreadPoolExecutor(
                max_workers=MAX_WORKERS,
                thread_name_prefix="llm-worker"
            ) as executor:
                futures = {
                    executor.submit(process_one_meeting, item): item["meeting_id"]
                    for item in items
                }
                for future in as_completed(futures):
                    mid = futures[future]
                    try:
                        result = future.result()
                        log.info(result)
                    except Exception as e:
                        log.error(f"[{mid}] Error: {e}", exc_info=True)

            log.info(f"Batch complete. Waiting {POLL_INTERVAL}s...")
            time.sleep(POLL_INTERVAL)

        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(30)

if __name__ == "__main__":
    main()
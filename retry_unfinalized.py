"""
retry_unfinalized.py
Finds all meetings in temp/live-doc-history/ that:
  1. Have state.json (were tracked)
  2. Do NOT have done.json (not finalized yet)
Tries to finalize them using 20 parallel workers.
Supports both old flat structure and new date-organized structure.
Run via systemd timer every 10 minutes.
"""

import os
import sys
import json
import time
import logging
import boto3
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

AWS_REGION  = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET   = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
DB_HOST     = os.environ.get("DB_HOST", "127.0.0.1")
DB_NAME     = os.environ.get("DB_NAME", "dochistory")
DB_USER     = os.environ.get("DB_USER", "postgres")
DB_PASS     = os.environ.get("DB_PASS", "DocHistory2026")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
IST_OFFSET  = timedelta(hours=5, minutes=30)
MAX_WORKERS = 20

DEPARTMENTS = [
    "Interview-Success", "Training", "Customer-Success", "Marketing",
    "COO", "CEO", "Executive-Assistant", "Techsphere", "HR",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("retry_unfinalized")

s3 = boto3.client("s3", region_name=AWS_REGION)


def get_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT,
        cursor_factory=psycopg2.extras.RealDictCursor
    )


def find_unfinalized_meetings() -> list[dict]:
    """
    Scan temp/live-doc-history/ for meetings with state.json but no done.json.
    Supports both structures:
      OLD: temp/live-doc-history/<meeting_id>/state.json
      NEW: temp/live-doc-history/<YYYY>/<Month-M>/<YYYY-MM-DD>/<meeting_id>/state.json
    Returns list of dicts: {meeting_id, prefix}
    """
    log.info("Scanning temp/live-doc-history/ for unfinalized meetings...")
    paginator  = s3.get_paginator("list_objects_v2")
    state_map  = {}   # meeting_id -> prefix
    done_set   = set()

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key   = obj["Key"]
            parts = key.split("/")
            # parts[0]=temp, parts[1]=live-doc-history

            # NEW structure: temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/meeting_id/file
            if len(parts) == 7 and parts[2].isdigit() and parts[3].startswith("Month-"):
                meeting_id = parts[5]
                prefix     = "/".join(parts[:6])
                filename   = parts[6]
                if filename == "state.json":
                    state_map[meeting_id] = prefix
                elif filename == "done.json":
                    done_set.add(meeting_id)

            # OLD structure: temp/live-doc-history/meeting_id/file
            elif len(parts) == 4 and parts[2].isdigit():
                meeting_id = parts[2]
                prefix     = "/".join(parts[:3])
                filename   = parts[3]
                if filename == "state.json":
                    if meeting_id not in state_map:
                        state_map[meeting_id] = prefix
                elif filename == "done.json":
                    done_set.add(meeting_id)

    unfinalized = [
        {"meeting_id": mid, "prefix": pfx}
        for mid, pfx in state_map.items()
        if mid not in done_set
    ]
    log.info(
        f"Found {len(state_map)} tracked, "
        f"{len(done_set)} finalized, "
        f"{len(unfinalized)} need retry"
    )
    return unfinalized


def find_final_s3_prefix(meeting_id: str) -> str | None:
    """meeting_id is the LAST folder of the recording path for every
    department, so the prefix is everything up to and including it."""
    paginator  = s3.get_paginator("list_objects_v2")
    search_str = f"/{meeting_id}/"
    for dept in DEPARTMENTS:
        found = set()
        try:
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{dept}/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if search_str not in key:
                        continue
                    parts = key.split("/")
                    try:
                        idx = parts.index(meeting_id)
                    except ValueError:
                        continue
                    if len(parts) > idx + 1:
                        found.add("/".join(parts[:idx + 1]))
            if found:
                return sorted(found)[0]
        except Exception as e:
            log.error(f"S3 search error in {dept}: {e}")
    return None


def copy_docs_to_final(meeting_id: str, temp_prefix: str, final_prefix: str) -> bool:
    src_key = f"{temp_prefix}/doc.txt"
    dst_key = f"{final_prefix}/docs/doc.txt"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=src_key)
    except Exception:
        log.warning(f"[{meeting_id}] No doc.txt at {src_key}")
        return False
    try:
        obj     = s3.get_object(Bucket=S3_BUCKET, Key=src_key)
        content = obj["Body"].read().decode("utf-8")
        content = content.replace(
            f"s3://{S3_BUCKET}/{src_key}",
            f"s3://{S3_BUCKET}/{dst_key}"
        )
        now_utc      = datetime.now(timezone.utc)
        now_ist      = (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST")
        finalized_at = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        content += (
            f"\n\n==================================================\n"
            f"FINALIZED AT: {finalized_at} ({now_ist})\n"
            f"Final S3 Path: s3://{S3_BUCKET}/{dst_key}\n"
        )
        s3.put_object(
            Bucket=S3_BUCKET, Key=dst_key,
            Body=content.encode("utf-8"), ContentType="text/plain"
        )
        log.info(f"[{meeting_id}] doc.txt → s3://{S3_BUCKET}/{dst_key}")
        return True
    except Exception as e:
        log.error(f"[{meeting_id}] Copy failed: {e}")
        return False


def create_done_json(meeting_id: str, temp_prefix: str, final_prefix: str):
    now_utc = datetime.now(timezone.utc)
    done = {
        "meeting_id":       meeting_id,
        "status":           "finalized",
        "finalized_at":     now_utc.isoformat(),
        "finalized_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "final_s3_prefix":  final_prefix,
        "final_doc_txt":    f"s3://{S3_BUCKET}/{final_prefix}/docs/doc.txt",
        "temp_prefix":      temp_prefix,
        "trigger":          "retry_unfinalized",
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{temp_prefix}/done.json",
        Body=json.dumps(done, indent=2),
        ContentType="application/json"
    )


def mark_finalized_in_db(meeting_id: str):
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE tracked_docs SET
                    status='finalized', is_active=FALSE, updated_at=NOW()
                WHERE meeting_id=%s
            """, (meeting_id,))
            conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"[{meeting_id}] DB update failed: {e}")


def process_one(item: dict) -> str:
    meeting_id  = item["meeting_id"]
    temp_prefix = item["prefix"]

    final_prefix = find_final_s3_prefix(meeting_id)
    if not final_prefix:
        return f"SKIP  {meeting_id} — recording not in S3 yet"

    success = copy_docs_to_final(meeting_id, temp_prefix, final_prefix)
    create_done_json(meeting_id, temp_prefix, final_prefix)
    mark_finalized_in_db(meeting_id)

    if success:
        return f"OK    {meeting_id} → {final_prefix}/docs/doc.txt"
    else:
        return f"DONE  {meeting_id} — no doc.txt but done.json created"


def main():
    log.info("=== retry_unfinalized starting ===")
    items = find_unfinalized_meetings()

    if not items:
        log.info("Nothing to retry. All tracked meetings are finalized.")
        return

    log.info(f"Retrying {len(items)} meetings with {MAX_WORKERS} parallel workers...")
    results = {"ok": 0, "skip": 0, "done": 0, "error": 0}
    skipped = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="retry") as executor:
        futures = {executor.submit(process_one, item): item["meeting_id"] for item in items}
        for future in as_completed(futures):
            mid = futures[future]
            try:
                result = future.result()
                log.info(result)
                if result.startswith("OK"):
                    results["ok"] += 1
                elif result.startswith("SKIP"):
                    results["skip"] += 1
                    skipped.append(mid)
                else:
                    results["done"] += 1
            except Exception as e:
                log.error(f"[{mid}] Error: {e}")
                results["error"] += 1

    log.info(f"""
=== retry_unfinalized complete ===
  Finalized:     {results['ok']}
  No doc.txt:    {results['done']}
  Still skipped: {results['skip']} (recording not in S3 yet)
  Errors:        {results['error']}
""")


if __name__ == "__main__":
    main()
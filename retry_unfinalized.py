"""
retry_unfinalized.py
--------------------
Finds all meetings in temp/live-doc-history/ that:
  1. Have state.json (were tracked)
  2. Do NOT have done.json (not finalized yet)
Then tries to finalize them using 50 parallel workers.

Run manually or on a cron schedule.
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
DB_PASS     = os.environ.get("DB_PASS", "")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
IST_OFFSET  = timedelta(hours=5, minutes=30)
MAX_WORKERS = 20

DEPARTMENTS = {
    "Interview-Success": 4,
    "Training":          2,
    "Customer-Success":  2,
    "Marketing":         2,
}

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


def find_unfinalized_meetings() -> list[str]:
    """Find all meeting_ids that have state.json but no done.json (within last 2 hours)."""
    log.info("Scanning temp/live-doc-history/ for unfinalized meetings...")
    paginator    = s3.get_paginator("list_objects_v2")
    has_state    = set()
    has_done     = set()
    state_times  = {}  # meeting_id -> last_modified

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key   = obj["Key"]
            parts = key.split("/")
            # temp/live-doc-history/<meeting_id>/state.json
            if len(parts) >= 4:
                meeting_id = parts[2]
                filename   = parts[3] if len(parts) > 3 else ""
                if filename == "state.json":
                    has_state.add(meeting_id)
                elif filename == "done.json":
                    has_done.add(meeting_id)

    unfinalized = sorted(has_state - has_done)
    log.info(f"Found {len(has_state)} tracked meetings, {len(has_done)} finalized, {len(unfinalized)} need retry")
    return unfinalized


def find_final_s3_prefix(meeting_id: str) -> str | None:
    paginator  = s3.get_paginator("list_objects_v2")
    search_str = f"/{meeting_id}/"

    for dept, offset in DEPARTMENTS.items():
        found = set()
        try:
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{dept}/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if search_str in key:
                        parts = key.split("/")
                        try:
                            idx = parts.index(meeting_id)
                            end = idx + offset + 1
                            if len(parts) > end:
                                found.add("/".join(parts[:end]))
                        except ValueError:
                            continue
            if found:
                return sorted(found)[0]
        except Exception as e:
            log.error(f"S3 search error in {dept}: {e}")
    return None


def copy_docs_to_final(meeting_id: str, final_prefix: str) -> bool:
    temp_prefix = f"temp/live-doc-history/{meeting_id}"
    docs_prefix = f"{final_prefix}/docs"
    src_key     = f"{temp_prefix}/doc.txt"
    dst_key     = f"{docs_prefix}/doc.txt"

    try:
        s3.head_object(Bucket=S3_BUCKET, Key=src_key)
    except Exception:
        log.warning(f"[{meeting_id}] No doc.txt found in temp")
        return False

    try:
        obj     = s3.get_object(Bucket=S3_BUCKET, Key=src_key)
        content = obj["Body"].read().decode("utf-8")
        content = content.replace(
            f"s3://{S3_BUCKET}/{temp_prefix}/doc.txt",
            f"s3://{S3_BUCKET}/{dst_key}"
        )
        now_ist      = (datetime.now(timezone.utc) + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST")
        finalized_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
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


def create_done_json(meeting_id: str, final_prefix: str):
    now_utc = datetime.now(timezone.utc)
    done = {
        "meeting_id":       meeting_id,
        "status":           "finalized",
        "finalized_at":     now_utc.isoformat(),
        "finalized_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "final_s3_prefix":  final_prefix,
        "final_doc_txt":    f"s3://{S3_BUCKET}/{final_prefix}/docs/doc.txt",
        "trigger":          "retry_unfinalized",
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"temp/live-doc-history/{meeting_id}/done.json",
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


def process_one(meeting_id: str) -> str:
    """Process a single meeting_id. Returns status string."""
    final_prefix = find_final_s3_prefix(meeting_id)
    if not final_prefix:
        return f"SKIP  {meeting_id} — recording folder not in S3 yet"

    success = copy_docs_to_final(meeting_id, final_prefix)
    create_done_json(meeting_id, final_prefix)
    mark_finalized_in_db(meeting_id)

    if success:
        return f"OK    {meeting_id} → {final_prefix}/docs/doc.txt"
    else:
        return f"DONE  {meeting_id} — no doc.txt but done.json created"


def main():
    log.info("=== retry_unfinalized starting ===")
    meeting_ids = find_unfinalized_meetings()

    if not meeting_ids:
        log.info("Nothing to retry. All tracked meetings are finalized.")
        return

    log.info(f"Retrying {len(meeting_ids)} meetings with {MAX_WORKERS} parallel workers...")

    results  = {"ok": 0, "skip": 0, "done": 0, "error": 0}
    skipped  = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="retry") as executor:
        futures = {executor.submit(process_one, mid): mid for mid in meeting_ids}
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
  Finalized:       {results['ok']}
  No doc.txt:      {results['done']}
  Still skipped:   {results['skip']} (recording not in S3 yet)
  Errors:          {results['error']}
""")

    if skipped:
        log.info(f"Still skipped (run again later): {skipped[:10]}{'...' if len(skipped)>10 else ''}")


if __name__ == "__main__":
    main()

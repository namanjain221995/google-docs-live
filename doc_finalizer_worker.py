"""
doc_finalizer_worker.py
Polls zoom-docs-finalize-queue.
When recording.completed fires:
  1. Waits 60s for recording processor
  2. Searches S3 for Interview-Success/<...>/<meeting_id>/ folder
  3. Finds temp prefix from DB or S3 scan (supports old + new structure)
  4. Copies doc.txt → Interview-Success/.../docs/doc.txt
  5. Creates done.json in temp folder
  6. Marks finalized in PostgreSQL

S3 Path Structures:
  NEW Interview-Success (current):
    Interview-Success/Host/Year/Month/Candidate/Company/Date/Round/MeetingID/FileType/file
    → Meeting_ID at parts[8], offset=0
    → final prefix = parts[:9]
    → doc.txt lands at: Interview-Success/.../MeetingID/docs/doc.txt

  OLD Interview-Success (before structure change):
    Interview-Success/Host/Year/Month/Candidate/MeetingID/Company/Date/Round/Time/FileType/file
    → Meeting_ID at parts[5], offset=4
    → final prefix = parts[:10]

  Training / Customer-Success / Marketing (unchanged):
    Dept/Host/Year/Month/Candidate/MeetingID/Date/Time/FileType/file
    → Meeting_ID at parts[5], offset=2
    → final prefix = parts[:8]
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

AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET           = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
DOCS_FINALIZE_QUEUE = os.environ.get("DOCS_FINALIZE_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/985100584614/zoom-docs-finalize-queue")
DB_HOST             = os.environ.get("DB_HOST", "127.0.0.1")
DB_NAME             = os.environ.get("DB_NAME", "dochistory")
DB_USER             = os.environ.get("DB_USER", "postgres")
DB_PASS             = os.environ.get("DB_PASS", "DocHistory2026")
DB_PORT             = int(os.environ.get("DB_PORT", "5432"))
WAIT_BEFORE_SEARCH  = int(os.environ.get("WAIT_BEFORE_SEARCH_SECONDS", "60"))
IST_OFFSET          = timedelta(hours=5, minutes=30)

# ── Departments ──
#  The recording Lambda places meeting_id as the LAST folder of the
#  path (just before the MP4/M4A/TRANSCRIPT/CHAT file-type folder) for
#  EVERY department. The final prefix is therefore everything up to and
#  including the meeting_id segment — no per-department offset needed.
DEPARTMENTS = [
    "Interview-Success", "Training", "Customer-Success", "Marketing",
    "COO", "CEO", "Executive-Assistant", "Techsphere", "HR",
]

os.makedirs("/home/ec2-user/google-docs-live/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ec2-user/google-docs-live/logs/doc_finalizer_worker.log"),
    ]
)
log = logging.getLogger("doc_finalizer_worker")

sqs = boto3.client("sqs", region_name=AWS_REGION)
s3  = boto3.client("s3",  region_name=AWS_REGION)


# ══════════════════════════════════════════════════════════════════════════════
#  Database helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT,
        cursor_factory=psycopg2.extras.RealDictCursor
    )


def get_temp_prefix_from_db(meeting_id: str) -> str | None:
    """Get temp_s3_prefix stored in DB when meeting started."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT temp_s3_prefix FROM tracked_docs WHERE meeting_id=%s LIMIT 1",
                (meeting_id,)
            )
            row = cur.fetchone()
        conn.close()
        if row and row.get("temp_s3_prefix"):
            return row["temp_s3_prefix"]
    except Exception as e:
        log.warning(f"DB lookup failed for meeting_id={meeting_id}: {e}")
    return None


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
        log.info(f"Marked meeting_id={meeting_id} finalized in DB")
    except Exception as e:
        log.warning(f"DB update failed (non-fatal): {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  S3 prefix helpers
# ══════════════════════════════════════════════════════════════════════════════

def find_temp_prefix_from_s3(meeting_id: str) -> str | None:
    """
    Search S3 for temp prefix of this meeting.
    Supports both structures:
      NEW: temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/<meeting_id>/
      OLD: temp/live-doc-history/<meeting_id>/
    """
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key   = obj["Key"]
            parts = key.split("/")

            # NEW: temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/meeting_id/file
            if (len(parts) >= 7
                    and parts[2].isdigit()
                    and parts[3].startswith("Month-")
                    and parts[5] == meeting_id):
                prefix = "/".join(parts[:6])
                log.info(f"Found temp prefix (NEW structure) for meeting_id={meeting_id}: {prefix}")
                return prefix

            # OLD: temp/live-doc-history/meeting_id/file
            if len(parts) >= 4 and parts[2] == meeting_id:
                prefix = "/".join(parts[:3])
                log.info(f"Found temp prefix (OLD structure) for meeting_id={meeting_id}: {prefix}")
                return prefix

    return None


def find_final_s3_prefix(meeting_id: str) -> str | None:
    """
    Search S3 across all departments for the final storage prefix
    matching this meeting_id.

    Uses per-department offset to build correct prefix depth:
      Interview-Success → offset=0 → prefix ends AT meeting_id folder
        NEW path: .../Candidate/Company/Date/Round/MeetingID/
      Training etc.     → offset=2 → prefix ends 2 folders after meeting_id
        path: .../Candidate/MeetingID/Date/Time/
    """
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
                result = sorted(found)[0]
                log.info(
                    f"Found final S3 prefix [{dept}] "
                    f"for meeting_id={meeting_id}: {result}"
                )
                return result

        except Exception as e:
            log.error(f"S3 search error in {dept}: {e}")

    return None


def check_already_finalized(meeting_id: str, temp_prefix: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=f"{temp_prefix}/done.json")
        return True
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  Copy + finalize helpers
# ══════════════════════════════════════════════════════════════════════════════

def copy_docs_to_final(meeting_id: str, temp_prefix: str, final_prefix: str) -> bool:
    """
    Copies doc.txt from temp prefix → final prefix/docs/doc.txt
    Also copies any images from temp prefix/images/ → final prefix/docs/images/
    """
    src_key = f"{temp_prefix}/doc.txt"
    dst_key = f"{final_prefix}/docs/doc.txt"

    # Check source exists
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=src_key)
    except Exception:
        log.warning(f"No temp doc.txt for meeting_id={meeting_id} at {src_key}")
        return False

    # Copy and update doc.txt content
    try:
        obj     = s3.get_object(Bucket=S3_BUCKET, Key=src_key)
        content = obj["Body"].read().decode("utf-8")

        # Update any internal S3 references
        content = content.replace(
            f"s3://{S3_BUCKET}/{src_key}",
            f"s3://{S3_BUCKET}/{dst_key}"
        )

        # Append finalization footer
        now_utc      = datetime.now(timezone.utc)
        now_ist      = (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST")
        finalized_at = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        content += (
            f"\n\n==================================================\n"
            f"FINALIZED AT: {finalized_at} ({now_ist})\n"
            f"Final S3 Path: s3://{S3_BUCKET}/{dst_key}\n"
        )

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=dst_key,
            Body=content.encode("utf-8"),
            ContentType="text/plain"
        )
        log.info(f"doc.txt copied → s3://{S3_BUCKET}/{dst_key}")

    except Exception as e:
        log.error(f"Copy failed for meeting_id={meeting_id}: {e}")
        return False

    # Copy images
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{temp_prefix}/images/"):
            for obj in page.get("Contents", []):
                fname = obj["Key"].split("/")[-1]
                s3.copy_object(
                    Bucket=S3_BUCKET,
                    CopySource={"Bucket": S3_BUCKET, "Key": obj["Key"]},
                    Key=f"{final_prefix}/docs/images/{fname}"
                )
        log.info(f"Images copied → {final_prefix}/docs/images/")
    except Exception as e:
        log.warning(f"Image copy failed (non-fatal) for meeting_id={meeting_id}: {e}")

    return True


def create_done_json(meeting_id: str, temp_prefix: str, final_prefix: str, msg: dict):
    now_utc = datetime.now(timezone.utc)
    done = {
        "meeting_id":       meeting_id,
        "status":           "finalized",
        "finalized_at":     now_utc.isoformat(),
        "finalized_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "final_s3_prefix":  final_prefix,
        "final_doc_txt":    f"s3://{S3_BUCKET}/{final_prefix}/docs/doc.txt",
        "temp_prefix":      temp_prefix,
        "host_email":       msg.get("host_email", ""),
        "topic":            msg.get("topic", ""),
        "trigger":          "recording.completed",
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{temp_prefix}/done.json",
        Body=json.dumps(done, indent=2),
        ContentType="application/json"
    )
    log.info(f"done.json created at s3://{S3_BUCKET}/{temp_prefix}/done.json")


def create_failed_json(meeting_id: str, temp_prefix: str, reason: str):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{temp_prefix}/done.json",
        Body=json.dumps({
            "meeting_id":   meeting_id,
            "status":       "failed",
            "reason":       reason,
            "finalized_at": datetime.now(timezone.utc).isoformat()
        }, indent=2),
        ContentType="application/json"
    )
    log.error(
        f"Failed done.json written for meeting_id={meeting_id} "
        f"at s3://{S3_BUCKET}/{temp_prefix}/done.json  reason={reason}"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Core message processor
# ══════════════════════════════════════════════════════════════════════════════

def process_finalize_message(msg: dict):
    meeting_id = str(msg.get("meeting_id", ""))
    if not meeting_id:
        log.warning(f"No meeting_id in finalize message: {msg}")
        return

    log.info(f"Received finalize trigger for meeting_id={meeting_id}")

    # ── Step 1: Get temp prefix — DB first, then S3 scan ──────────────────
    temp_prefix = get_temp_prefix_from_db(meeting_id)
    if not temp_prefix:
        log.info(f"Not found in DB, scanning S3 for meeting_id={meeting_id}")
        temp_prefix = find_temp_prefix_from_s3(meeting_id)

    if not temp_prefix:
        log.info(f"meeting_id={meeting_id} was not tracked — skipping")
        return

    # ── Step 2: Already finalized? ─────────────────────────────────────────
    if check_already_finalized(meeting_id, temp_prefix):
        log.info(f"meeting_id={meeting_id} already finalized — skipping")
        return

    # ── Step 3: Wait for recording processor to finish ─────────────────────
    log.info(f"Waiting {WAIT_BEFORE_SEARCH}s for recording processor to finish...")
    time.sleep(WAIT_BEFORE_SEARCH)

    # ── Step 4: Search for final S3 path — retry up to 6 times ───────────
    final_prefix = None
    for attempt in range(6):
        final_prefix = find_final_s3_prefix(meeting_id)
        if final_prefix:
            break
        log.info(f"Attempt {attempt + 1}/6: final prefix not found yet, waiting 2 min...")
        time.sleep(120)

    if not final_prefix:
        log.error(
            f"Could not find final S3 path for meeting_id={meeting_id} "
            f"after 6 attempts"
        )
        create_failed_json(meeting_id, temp_prefix, "final_s3_prefix_not_found")
        return

    # ── Step 5: Copy doc.txt + images → final path ────────────────────────
    copy_docs_to_final(meeting_id, temp_prefix, final_prefix)

    # ── Step 6: Write done.json ────────────────────────────────────────────
    create_done_json(meeting_id, temp_prefix, final_prefix, msg)

    # ── Step 7: Mark finalized in DB ──────────────────────────────────────
    mark_finalized_in_db(meeting_id)

    log.info(
        f"✅ FINALIZED meeting_id={meeting_id} "
        f"→ s3://{S3_BUCKET}/{final_prefix}/docs/doc.txt"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  SQS polling loop
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("doc_finalizer_worker starting...")
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=DOCS_FINALIZE_QUEUE,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20,
                VisibilityTimeout=600
            )
            messages = resp.get("Messages", [])
            if not messages:
                continue

            for msg in messages:
                receipt = msg["ReceiptHandle"]
                try:
                    body = json.loads(msg["Body"])
                    # Unwrap SNS envelope if present
                    if "Message" in body:
                        body = json.loads(body["Message"])
                    process_finalize_message(body)
                    sqs.delete_message(
                        QueueUrl=DOCS_FINALIZE_QUEUE,
                        ReceiptHandle=receipt
                    )
                except Exception as e:
                    log.error(f"Error processing message: {e}", exc_info=True)

        except Exception as e:
            log.error(f"Outer loop error: {e}", exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    main()
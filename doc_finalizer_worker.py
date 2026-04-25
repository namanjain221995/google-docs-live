"""
doc_finalizer_worker.py
-----------------------
Polls zoom-docs-finalize-queue.
When recording.completed fires for a meeting:
  1. Waits 60 seconds for zoom-recording-processor to finish uploading to S3
  2. Searches S3 for Interview-Success/<...>/<meeting_id>/<...>/ folder
  3. Copies temp/live-doc-history/<meeting_id>/doc.txt
     → Interview-Success/<...>/<meeting_id>/<...>/docs/doc.txt
  4. Creates temp/live-doc-history/<meeting_id>/done.json
     with final path info
  5. Marks tracked_doc as finalized in PostgreSQL
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

AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET             = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
DOCS_FINALIZE_QUEUE   = os.environ.get("DOCS_FINALIZE_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/985100584614/zoom-docs-finalize-queue")
DB_HOST               = os.environ.get("DB_HOST", "127.0.0.1")
DB_NAME               = os.environ.get("DB_NAME", "dochistory")
DB_USER               = os.environ.get("DB_USER", "postgres")
DB_PASS               = os.environ.get("DB_PASS", "")
DB_PORT               = int(os.environ.get("DB_PORT", "5432"))
WAIT_BEFORE_SEARCH    = int(os.environ.get("WAIT_BEFORE_SEARCH_SECONDS", "60"))
IST_OFFSET            = timedelta(hours=5, minutes=30)

# Departments and their S3 path depth after meeting_id
DEPARTMENTS = {
    "Interview-Success": 4,   # MeetingID/Company/Date/Round/Time
    "Training":          2,   # MeetingID/Date/Time
    "Customer-Success":  2,
    "Marketing":         2,
}

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


def get_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT,
        cursor_factory=psycopg2.extras.RealDictCursor
    )


def find_final_s3_prefix(meeting_id: str) -> str | None:
    """Search all departments for the recording folder of this meeting_id."""
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
                result = sorted(found)[0]
                log.info(f"Found final S3 prefix [{dept}] for meeting_id={meeting_id}: {result}")
                return result
        except Exception as e:
            log.error(f"S3 search error in {dept} for meeting_id={meeting_id}: {e}")

    log.warning(f"No final S3 prefix found for meeting_id={meeting_id}")
    return None


def copy_docs_to_final(meeting_id: str, final_prefix: str) -> bool:
    """Copy doc.txt and images from temp to final Interview-Success path."""
    temp_prefix = f"temp/live-doc-history/{meeting_id}"
    docs_prefix = f"{final_prefix}/docs"
    src_key     = f"{temp_prefix}/doc.txt"
    dst_key     = f"{docs_prefix}/doc.txt"

    # Check if temp doc.txt exists
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=src_key)
    except Exception:
        log.warning(f"No temp doc.txt found for meeting_id={meeting_id} at {src_key}")
        return False

    # Read, update S3 location line, write to final
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
        log.info(f"doc.txt copied to final: s3://{S3_BUCKET}/{dst_key}")
    except Exception as e:
        log.error(f"Failed to copy doc.txt for meeting_id={meeting_id}: {e}")
        return False

    # Copy images if any
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{temp_prefix}/images/"):
            for obj in page.get("Contents", []):
                fname = obj["Key"].split("/")[-1]
                s3.copy_object(
                    Bucket=S3_BUCKET,
                    CopySource={"Bucket": S3_BUCKET, "Key": obj["Key"]},
                    Key=f"{docs_prefix}/images/{fname}"
                )
                log.info(f"Image copied: {fname}")
    except Exception as e:
        log.warning(f"Image copy warning: {e}")

    return True


def create_done_json(meeting_id: str, final_prefix: str, msg: dict):
    """
    Create done.json in temp folder with full finalization info.
    This confirms docs were successfully moved to final location.
    """
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc + IST_OFFSET

    done = {
        "meeting_id":       meeting_id,
        "status":           "finalized",
        "finalized_at":     now_utc.isoformat(),
        "finalized_at_ist": now_ist.strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "final_s3_prefix":  final_prefix,
        "final_doc_txt":    f"s3://{S3_BUCKET}/{final_prefix}/docs/doc.txt",
        "temp_prefix":      f"temp/live-doc-history/{meeting_id}",
        "host_email":       msg.get("host_email", ""),
        "topic":            msg.get("topic", ""),
        "trigger":          "recording.completed",
    }

    key = f"temp/live-doc-history/{meeting_id}/done.json"
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(done, indent=2), ContentType="application/json"
    )
    log.info(f"done.json created: s3://{S3_BUCKET}/{key}")
    return done


def mark_finalized_in_db(meeting_id: str):
    """Mark the tracked_doc as finalized in PostgreSQL."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE tracked_docs SET
                    status='finalized',
                    is_active=FALSE,
                    updated_at=NOW()
                WHERE meeting_id=%s
            """, (meeting_id,))
            conn.commit()
        conn.close()
        log.info(f"Marked meeting_id={meeting_id} as finalized in DB")
    except Exception as e:
        log.warning(f"DB update failed (non-fatal): {e}")


def check_temp_exists(meeting_id: str) -> bool:
    """Check if we were tracking this meeting at all."""
    try:
        s3.head_object(
            Bucket=S3_BUCKET,
            Key=f"temp/live-doc-history/{meeting_id}/state.json"
        )
        return True
    except Exception:
        return False


def process_finalize_message(msg: dict):
    """Main logic for one finalization message."""
    meeting_id = str(msg.get("meeting_id", ""))
    if not meeting_id:
        log.warning(f"No meeting_id in finalize message: {msg}")
        return

    log.info(f"Received finalize trigger for meeting_id={meeting_id}")

    # Check if we were tracking this meeting
    if not check_temp_exists(meeting_id):
        log.info(f"meeting_id={meeting_id} was not tracked (no state.json) — skipping")
        return

    # Check if already finalized
    try:
        s3.head_object(
            Bucket=S3_BUCKET,
            Key=f"temp/live-doc-history/{meeting_id}/done.json"
        )
        log.info(f"meeting_id={meeting_id} already finalized (done.json exists)")
        return
    except Exception:
        pass  # not finalized yet, continue

    # Wait for recording processor to finish uploading to S3
    log.info(f"Waiting {WAIT_BEFORE_SEARCH}s for recording processor to finish S3 upload...")
    time.sleep(WAIT_BEFORE_SEARCH)

    # Search for final S3 path — retry up to 6 times (10 min total)
    final_prefix = None
    for attempt in range(6):
        final_prefix = find_final_s3_prefix(meeting_id)
        if final_prefix:
            break
        log.info(f"Attempt {attempt+1}/6: Final path not found yet, waiting 2 min...")
        time.sleep(120)

    if not final_prefix:
        log.error(f"Could not find final S3 path for meeting_id={meeting_id} after 6 attempts")
        # Create a failed done.json so we don't retry forever
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"temp/live-doc-history/{meeting_id}/done.json",
            Body=json.dumps({
                "meeting_id": meeting_id,
                "status": "failed",
                "reason": "final_s3_prefix_not_found",
                "finalized_at": datetime.now(timezone.utc).isoformat()
            }, indent=2),
            ContentType="application/json"
        )
        return

    # Copy docs to final path
    success = copy_docs_to_final(meeting_id, final_prefix)
    if not success:
        log.warning(f"doc.txt copy failed for meeting_id={meeting_id} — may not have been tracked")

    # Always create done.json
    create_done_json(meeting_id, final_prefix, msg)

    # Mark finalized in DB
    mark_finalized_in_db(meeting_id)

    log.info(
        f"FINALIZED meeting_id={meeting_id}\n"
        f"  doc.txt → s3://{S3_BUCKET}/{final_prefix}/docs/doc.txt\n"
        f"  done.json → s3://{S3_BUCKET}/temp/live-doc-history/{meeting_id}/done.json"
    )


def main():
    log.info("doc_finalizer_worker starting...")
    log.info(f"Queue: {DOCS_FINALIZE_QUEUE}")
    log.info(f"Wait before search: {WAIT_BEFORE_SEARCH}s")

    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=DOCS_FINALIZE_QUEUE,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20,
                VisibilityTimeout=600  # 10 min — enough for retries
            )
            messages = resp.get("Messages", [])
            if not messages:
                continue

            for msg in messages:
                receipt = msg["ReceiptHandle"]
                try:
                    body = json.loads(msg["Body"])
                    # Handle SNS wrapper if present
                    if "Message" in body:
                        body = json.loads(body["Message"])

                    process_finalize_message(body)
                    sqs.delete_message(
                        QueueUrl=DOCS_FINALIZE_QUEUE,
                        ReceiptHandle=receipt
                    )
                except Exception as e:
                    log.error(f"Error processing finalize message: {e}", exc_info=True)

        except Exception as e:
            log.error(f"Outer loop error: {e}", exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    main()
"""
google_change_worker.py
-----------------------
Dynamic worker pool for processing Google Doc changes.

Design:
- Main process polls google-doc-change-queue (SQS)
- Each unique doc_id gets its own dedicated worker (via concurrent.futures thread pool)
- If a doc has NO updates for 30 minutes → worker marks it IDLE and exits
- Workers re-spawn automatically when a new change arrives for an idle doc
- Handles text diffs (ADDED/REMOVED) and image detection
- Writes doc.txt and image files to S3
"""

import os
import sys
import json
import time
import logging
import hashlib
import threading
import boto3
import psycopg2
import psycopg2.extras
import difflib
import re
import io
import uuid
import base64
import requests
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, Future
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
AWS_REGION           = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET            = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
CHANGE_QUEUE_URL     = os.environ.get("CHANGE_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/985100584614/google-doc-change-queue")
GOOGLE_SECRET_NAME   = os.environ.get("GOOGLE_SECRET_NAME", "google/doc-history-service-account")
DB_HOST              = os.environ.get("DB_HOST", "localhost")
DB_NAME              = os.environ.get("DB_NAME", "dochistory")
DB_USER              = os.environ.get("DB_USER", "postgres")
DB_PASS              = os.environ.get("DB_PASS", "")
DB_PORT              = int(os.environ.get("DB_PORT", "5432"))

IDLE_TIMEOUT_SECONDS = 30 * 60   # 30 minutes → worker exits
MAX_WORKERS          = 250        # thread pool ceiling (200 docs + headroom)
IST_OFFSET           = timedelta(hours=5, minutes=30)

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
os.makedirs("/home/ec2-user/doc-ui-worker/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ec2-user/doc-ui-worker/logs/google_change_worker.log"),
    ]
)
log = logging.getLogger("google_change_worker")

# ──────────────────────────────────────────────
# AWS CLIENTS
# ──────────────────────────────────────────────
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3  = boto3.client("s3",  region_name=AWS_REGION)
sm  = boto3.client("secretsmanager", region_name=AWS_REGION)

# ──────────────────────────────────────────────
# SECRETS + GOOGLE AUTH
# ──────────────────────────────────────────────
_google_creds = None
_google_lock  = threading.Lock()

def get_google_creds():
    global _google_creds
    with _google_lock:
        if _google_creds:
            return _google_creds
        resp   = sm.get_secret_value(SecretId=GOOGLE_SECRET_NAME)
        secret = json.loads(resp["SecretString"])
        _google_creds = service_account.Credentials.from_service_account_info(
            secret, scopes=GOOGLE_SCOPES
        )
    return _google_creds

def get_drive_service():
    return build("drive", "v3", credentials=get_google_creds(), cache_discovery=False)

def get_docs_service():
    return build("docs", "v1", credentials=get_google_creds(), cache_discovery=False)

# ──────────────────────────────────────────────
# DB HELPERS (per-thread connections)
# ──────────────────────────────────────────────
_thread_local = threading.local()

def get_db():
    if not hasattr(_thread_local, "conn") or _thread_local.conn.closed:
        _thread_local.conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS, port=DB_PORT,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    return _thread_local.conn

def get_tracked_doc_by_doc_id(doc_id: str) -> dict | None:
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM tracked_docs WHERE doc_id = %s AND is_active = TRUE LIMIT 1",
            (doc_id,)
        )
        return cur.fetchone()

def get_tracked_doc_by_meeting(meeting_id: str) -> dict | None:
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM tracked_docs WHERE meeting_id = %s AND is_active = TRUE LIMIT 1",
            (meeting_id,)
        )
        return cur.fetchone()

def get_all_active_docs() -> list:
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM tracked_docs WHERE is_active = TRUE AND status = 'active'")
        return cur.fetchall()

def mark_doc_idle(meeting_id: str):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tracked_docs SET status = 'idle', updated_at = NOW() WHERE meeting_id = %s",
            (meeting_id,)
        )
        conn.commit()
    log.info(f"Marked meeting_id={meeting_id} as IDLE (30-min timeout)")

def update_last_change(meeting_id: str):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tracked_docs SET last_change_at = NOW(), status = 'active', updated_at = NOW() WHERE meeting_id = %s",
            (meeting_id,)
        )
        conn.commit()

def get_next_version(meeting_id: str, doc_id: str) -> int:
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(version_number), 0) FROM doc_snapshots WHERE meeting_id = %s AND doc_id = %s",
            (meeting_id, doc_id)
        )
        row = cur.fetchone()
        return (row[0] if row else 0) + 1

def save_snapshot(meeting_id: str, doc_id: str, version: int, content: str, edited_by: str):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO doc_snapshots (meeting_id, doc_id, version_number, content_text, edited_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (meeting_id, doc_id, version, content, edited_by))
        conn.commit()

def get_last_snapshot_content(meeting_id: str, doc_id: str) -> str | None:
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT content_text FROM doc_snapshots
            WHERE meeting_id = %s AND doc_id = %s
            ORDER BY version_number DESC LIMIT 1
        """, (meeting_id, doc_id))
        row = cur.fetchone()
        return row["content_text"] if row else None

# ──────────────────────────────────────────────
# GOOGLE DOCS TEXT EXTRACTION
# ──────────────────────────────────────────────
def extract_text_from_doc(doc_id: str) -> tuple[str, list]:
    """
    Returns (plain_text, image_refs_list)
    image_refs_list = [{"inline_object_id": ..., "title": ...}, ...]
    """
    docs = get_docs_service()
    doc  = docs.documents().get(documentId=doc_id).execute()

    text_parts   = []
    image_refs   = []

    body_content = doc.get("body", {}).get("content", [])

    for element in body_content:
        if "paragraph" in element:
            para = element["paragraph"]
            for pe in para.get("elements", []):
                if "textRun" in pe:
                    text_parts.append(pe["textRun"].get("content", ""))
                elif "inlineObjectElement" in pe:
                    obj_id = pe["inlineObjectElement"].get("inlineObjectId", "")
                    if obj_id:
                        image_refs.append({"inline_object_id": obj_id})

        elif "table" in element:
            for row in element["table"].get("tableRows", []):
                for cell in row.get("tableCells", []):
                    for cell_el in cell.get("content", []):
                        if "paragraph" in cell_el:
                            for pe in cell_el["paragraph"].get("elements", []):
                                if "textRun" in pe:
                                    text_parts.append(pe["textRun"].get("content", ""))

    # Enrich image refs with source URIs from inlineObjects
    inline_objects = doc.get("inlineObjects", {})
    enriched_images = []
    for ref in image_refs:
        obj_id = ref["inline_object_id"]
        obj    = inline_objects.get(obj_id, {})
        props  = obj.get("inlineObjectProperties", {}).get("embeddedObject", {})
        uri    = props.get("imageProperties", {}).get("sourceUri", "")
        title  = props.get("title", "")
        enriched_images.append({
            "inline_object_id": obj_id,
            "source_uri": uri,
            "title": title
        })

    plain_text = "".join(text_parts)
    return plain_text, enriched_images

# ──────────────────────────────────────────────
# GET LAST EDITOR FROM DRIVE REVISIONS
# ──────────────────────────────────────────────
def get_last_editor(doc_id: str) -> str:
    try:
        drive = get_drive_service()
        revisions = drive.revisions().list(fileId=doc_id, fields="revisions(lastModifyingUser,modifiedTime)").execute()
        rev_list = revisions.get("revisions", [])
        if rev_list:
            last = rev_list[-1]
            user = last.get("lastModifyingUser", {})
            return user.get("emailAddress", user.get("displayName", "Unknown"))
    except Exception as e:
        log.warning(f"Could not get last editor for doc_id={doc_id}: {e}")
    return "Unknown"

# ──────────────────────────────────────────────
# TEXT DIFF
# ──────────────────────────────────────────────
def compute_diff(old_text: str, new_text: str) -> tuple[list, list]:
    """Returns (added_lines, removed_lines)"""
    old_lines = [l for l in old_text.splitlines() if l.strip()]
    new_lines = [l for l in new_text.splitlines() if l.strip()]

    differ  = difflib.unified_diff(old_lines, new_lines, lineterm="")
    added   = []
    removed = []

    for line in differ:
        if line.startswith("+") and not line.startswith("+++"):
            added.append(line[1:].strip())
        elif line.startswith("-") and not line.startswith("---"):
            removed.append(line[1:].strip())

    return added, removed

# ──────────────────────────────────────────────
# IMAGE UPLOAD TO S3
# ──────────────────────────────────────────────
def download_and_upload_image(source_uri: str, meeting_id: str, img_index: int) -> str | None:
    """
    Downloads image from Google and uploads to S3.
    Returns the S3 key or None on failure.
    """
    try:
        creds = get_google_creds()
        # Refresh token if needed
        import google.auth.transport.requests as google_requests
        creds.refresh(google_requests.Request())
        headers = {"Authorization": f"Bearer {creds.token}"}

        resp = requests.get(source_uri, headers=headers, timeout=15)
        if resp.status_code != 200:
            log.warning(f"Image download failed status={resp.status_code}")
            return None

        # Detect extension from content-type
        content_type = resp.headers.get("content-type", "image/png")
        ext = "png"
        if "jpeg" in content_type or "jpg" in content_type:
            ext = "jpg"
        elif "gif" in content_type:
            ext = "gif"
        elif "webp" in content_type:
            ext = "webp"

        now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        s3_key   = f"temp/live-doc-history/{meeting_id}/images/{now_str}_img_{img_index}.{ext}"

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=resp.content,
            ContentType=content_type
        )
        log.info(f"Image uploaded to s3://{S3_BUCKET}/{s3_key}")
        return s3_key

    except Exception as e:
        log.error(f"Image upload failed: {e}")
        return None

# ──────────────────────────────────────────────
# READ / WRITE doc.txt FROM S3
# ──────────────────────────────────────────────
def read_doc_txt(prefix: str) -> str:
    key = f"{prefix}/doc.txt"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        return ""
    except Exception as e:
        log.error(f"Failed to read doc.txt from S3: {e}")
        return ""

def write_doc_txt(prefix: str, content: str):
    key = f"{prefix}/doc.txt"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="text/plain"
    )

def strip_current_final_block(doc_txt: str) -> str:
    """Remove the trailing CURRENT FINAL CONTENT block so we can replace it."""
    marker = "\n==================================================\nCURRENT FINAL CONTENT\n"
    idx = doc_txt.find(marker)
    if idx != -1:
        return doc_txt[:idx]
    return doc_txt

# ──────────────────────────────────────────────
# PROCESS ONE DOC CHANGE
# ──────────────────────────────────────────────
def process_doc_change(doc_record: dict):
    meeting_id  = doc_record["meeting_id"]
    doc_id      = doc_record["doc_id"]
    doc_url     = doc_record.get("doc_url", "")
    candidate   = doc_record.get("candidate", "Unknown")
    company     = doc_record.get("company", "Unknown")
    prefix      = doc_record.get("temp_s3_prefix", f"temp/live-doc-history/{meeting_id}")

    log.info(f"Processing change for meeting_id={meeting_id} doc_id={doc_id}")

    # Fetch latest doc content
    new_text, new_images = extract_text_from_doc(doc_id)
    if not new_text.strip():
        log.info(f"Empty doc content for doc_id={doc_id}, skipping")
        return

    # Get previous snapshot
    old_text = get_last_snapshot_content(meeting_id, doc_id) or ""

    # Compute diff
    added, removed = compute_diff(old_text, new_text)

    # If nothing changed (duplicate notification), skip
    if not added and not removed and old_text:
        log.info(f"No text change detected for doc_id={doc_id}, skipping")
        return

    # Get editor info
    edited_by = get_last_editor(doc_id)

    # IST timestamp
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc + IST_OFFSET
    edited_at_str = now_ist.strftime("%Y-%m-%d %I:%M %p IST")

    # Detect image changes
    # We compare inline_object_ids from previous snapshot via DB
    # For now: if new_images is non-empty and version is 1 (baseline), note them
    # For subsequent versions: compare image object IDs
    image_added_refs   = []
    image_removed_refs = []

    conn = get_db()
    version = get_next_version(meeting_id, doc_id)

    # If this is a baseline (version 1), all images are "existing" not "added"
    if version > 1 and new_images:
        for i, img in enumerate(new_images, 1):
            uri = img.get("source_uri", "")
            if uri:
                s3_key = download_and_upload_image(uri, meeting_id, i)
                if s3_key:
                    image_added_refs.append(s3_key)

    # Build version block
    version_block = f"""
==================================================
VERSION {version}
Edited At: {edited_at_str}
Edited By: {edited_by}

ADDED:
"""
    if added:
        for line in added:
            version_block += f"- {line}\n"
    else:
        version_block += "- (nothing added)\n"

    if image_added_refs:
        for ref in image_added_refs:
            version_block += f"- IMAGE_ADDED: {ref}\n"

    version_block += "\nREMOVED:\n"
    if removed:
        for line in removed:
            version_block += f"- {line}\n"
    else:
        version_block += "- (nothing removed)\n"

    if image_removed_refs:
        for ref in image_removed_refs:
            version_block += f"- IMAGE_REMOVED: {ref}\n"

    # Build current snapshot text (with image placeholders)
    snapshot_text = new_text
    for img in new_images:
        obj_id = img.get("inline_object_id", "")
        snapshot_text += f"\n[IMAGE: {obj_id}]"

    version_block += f"\nCURRENT SNAPSHOT:\n{snapshot_text.strip()}\n"

    # Read existing doc.txt, append version block, update CURRENT FINAL CONTENT
    existing_doc_txt = read_doc_txt(prefix)

    # If this is baseline, initialize proper header
    if version == 1 and "TRACKING INITIALIZED" in existing_doc_txt:
        # Keep header, replace body
        header_end = existing_doc_txt.find("==================================================\nTRACKING INITIALIZED")
        if header_end != -1:
            existing_doc_txt = existing_doc_txt[:header_end]

    stripped = strip_current_final_block(existing_doc_txt)
    new_doc_txt = stripped + version_block + f"""
==================================================
CURRENT FINAL CONTENT
{new_text.strip()}
"""

    # Write back to S3
    write_doc_txt(prefix, new_doc_txt)

    # Save snapshot to DB
    save_snapshot(meeting_id, doc_id, version, new_text, edited_by)

    # Update last_change timestamp
    update_last_change(meeting_id)

    log.info(f"doc.txt updated for meeting_id={meeting_id} version={version} added={len(added)} removed={len(removed)}")

# ──────────────────────────────────────────────
# WORKER STATE TRACKING
# ──────────────────────────────────────────────
# Maps doc_id -> {"last_activity": timestamp, "meeting_id": str}
_active_workers: dict[str, dict] = {}
_workers_lock   = threading.Lock()

def worker_loop(doc_id: str, meeting_id: str):
    """
    Dedicated worker loop per doc.
    Sleeps between processing, exits after 30-min idle.
    """
    thread_name = threading.current_thread().name
    log.info(f"Worker started for doc_id={doc_id} meeting_id={meeting_id} thread={thread_name}")

    with _workers_lock:
        _active_workers[doc_id] = {
            "last_activity": time.time(),
            "meeting_id": meeting_id
        }

    while True:
        # Check idle timeout
        with _workers_lock:
            last_activity = _active_workers.get(doc_id, {}).get("last_activity", 0)

        idle_seconds = time.time() - last_activity
        if idle_seconds >= IDLE_TIMEOUT_SECONDS:
            log.info(f"Worker for doc_id={doc_id} idle for {idle_seconds:.0f}s — exiting")
            mark_doc_idle(meeting_id)
            with _workers_lock:
                _active_workers.pop(doc_id, None)
            return

        # Check if there's a pending signal for this doc
        with _workers_lock:
            pending = _active_workers.get(doc_id, {}).get("pending", False)

        if pending:
            with _workers_lock:
                _active_workers[doc_id]["pending"] = False
                _active_workers[doc_id]["last_activity"] = time.time()

            try:
                # Re-fetch doc record from DB (status may have changed)
                doc_record = get_tracked_doc_by_doc_id(doc_id)
                if doc_record and doc_record["is_active"]:
                    process_doc_change(doc_record)
                else:
                    log.info(f"doc_id={doc_id} is no longer active, worker exiting")
                    with _workers_lock:
                        _active_workers.pop(doc_id, None)
                    return
            except Exception as e:
                log.error(f"Worker error for doc_id={doc_id}: {e}", exc_info=True)

        time.sleep(2)  # poll internal state every 2 seconds

# ──────────────────────────────────────────────
# SIGNAL A WORKER (or spawn new one)
# ──────────────────────────────────────────────
def signal_or_spawn_worker(executor: ThreadPoolExecutor, doc_id: str, meeting_id: str):
    with _workers_lock:
        if doc_id in _active_workers:
            # Signal existing worker
            _active_workers[doc_id]["pending"] = True
            _active_workers[doc_id]["last_activity"] = time.time()
            log.info(f"Signaled existing worker for doc_id={doc_id}")
        else:
            # Spawn new worker
            _active_workers[doc_id] = {
                "last_activity": time.time(),
                "meeting_id": meeting_id,
                "pending": True
            }
            executor.submit(worker_loop, doc_id, meeting_id)
            log.info(f"Spawned new worker for doc_id={doc_id} meeting_id={meeting_id}")

# ──────────────────────────────────────────────
# PARSE INCOMING SQS MESSAGE
# ──────────────────────────────────────────────
def parse_change_message(msg_body: str) -> dict | None:
    """
    Google webhook → API Gateway → SQS.
    The SQS body may contain:
    - A Google Drive notification with headers passed as body
    - Or just a wake-up signal with meeting_id in the message attributes
    We extract doc_id from the Google headers or from our DB by channel_id.
    """
    try:
        body = json.loads(msg_body)

        # Check if it has doc_id directly (future enhancement)
        if "doc_id" in body:
            return body

        # Google sends resource state change notifications with resourceId
        # API Gateway can forward headers as body fields
        resource_id  = body.get("resourceId", body.get("X-Goog-Resource-ID", ""))
        channel_id   = body.get("channelId", body.get("X-Goog-Channel-ID", ""))
        resource_uri = body.get("resourceUri", "")

        # Extract doc_id from resourceUri if present
        # URI format: https://www.googleapis.com/drive/v3/files/DOC_ID
        doc_id = ""
        if resource_uri:
            parts = resource_uri.rstrip("/").split("/")
            doc_id = parts[-1] if parts else ""

        # Fallback: lookup by channel_id in DB
        if not doc_id and channel_id:
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT doc_id, meeting_id FROM doc_watch_state WHERE watch_channel_id = %s LIMIT 1",
                    (channel_id,)
                )
                row = cur.fetchone()
                if row:
                    doc_id = row["doc_id"]

        if doc_id:
            return {"doc_id": doc_id}

    except Exception as e:
        log.warning(f"Could not parse change message: {e} body={msg_body[:200]}")

    return None

# ──────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────
def main():
    log.info(f"google_change_worker starting — max_workers={MAX_WORKERS} idle_timeout={IDLE_TIMEOUT_SECONDS}s")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="doc-worker") as executor:
        while True:
            try:
                resp = sqs.receive_message(
                    QueueUrl=CHANGE_QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=90
                )
                messages = resp.get("Messages", [])

                for msg in messages:
                    receipt = msg["ReceiptHandle"]
                    try:
                        parsed = parse_change_message(msg["Body"])

                        if not parsed or not parsed.get("doc_id"):
                            log.warning(f"Could not extract doc_id from SQS message, skipping")
                            sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)
                            continue

                        doc_id = parsed["doc_id"]

                        # Look up tracked doc
                        doc_record = get_tracked_doc_by_doc_id(doc_id)
                        if not doc_record:
                            log.info(f"doc_id={doc_id} not in tracked_docs, ignoring")
                            sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)
                            continue

                        meeting_id = doc_record["meeting_id"]

                        # Signal or spawn worker
                        signal_or_spawn_worker(executor, doc_id, meeting_id)

                        # Delete message after signaling
                        sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)

                    except Exception as e:
                        log.error(f"Error handling SQS message: {e}", exc_info=True)

            except Exception as e:
                log.error(f"Outer loop error: {e}", exc_info=True)
                time.sleep(5)

if __name__ == "__main__":
    main()

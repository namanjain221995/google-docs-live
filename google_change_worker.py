"""
google_change_worker.py
Dynamic worker pool for processing Google Doc changes.

- Polls google-doc-change-queue (SQS)
- Each doc_id gets its own dedicated thread worker
- 30-min idle timeout -> marks doc IDLE -> copies doc.txt to final Interview-Success path
- Idle retry loop checks every 10 min for docs whose recording folder now exists
- Handles text diffs (ADDED/REMOVED)
- Writes doc.txt to S3 temp path
"""

import os
import sys
import json
import time
import logging
import threading
import boto3
import psycopg2
import psycopg2.extras
import difflib
import io
import uuid
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ── CONFIG ──
AWS_REGION         = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET          = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
CHANGE_QUEUE_URL   = os.environ.get("CHANGE_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/985100584614/google-doc-change-queue")
GOOGLE_SECRET_NAME = os.environ.get("GOOGLE_SECRET_NAME", "google/doc-history-service-account")
DB_HOST            = os.environ.get("DB_HOST", "127.0.0.1")
DB_NAME            = os.environ.get("DB_NAME", "dochistory")
DB_USER            = os.environ.get("DB_USER", "postgres")
DB_PASS            = os.environ.get("DB_PASS", "")
DB_PORT            = int(os.environ.get("DB_PORT", "5432"))
IDLE_TIMEOUT       = 30 * 60   # 30 minutes
MAX_WORKERS        = 250
IST_OFFSET         = timedelta(hours=5, minutes=30)
GOOGLE_SCOPES      = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ── LOGGING ──
os.makedirs("/home/ec2-user/google-docs-live/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ec2-user/google-docs-live/logs/google_change_worker.log"),
    ]
)
log = logging.getLogger("google_change_worker")

# ── AWS CLIENTS ──
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3  = boto3.client("s3",  region_name=AWS_REGION)
sm  = boto3.client("secretsmanager", region_name=AWS_REGION)

# ── GOOGLE AUTH ──
_google_creds = None
_google_lock  = threading.Lock()

def get_google_creds():
    global _google_creds
    with _google_lock:
        if _google_creds:
            return _google_creds
        resp          = sm.get_secret_value(SecretId=GOOGLE_SECRET_NAME)
        secret        = json.loads(resp["SecretString"])
        _google_creds = service_account.Credentials.from_service_account_info(
            secret, scopes=GOOGLE_SCOPES)
    return _google_creds

def get_drive_service():
    return build("drive", "v3", credentials=get_google_creds(), cache_discovery=False)

def get_docs_service():
    return build("docs", "v1", credentials=get_google_creds(), cache_discovery=False)

# ── DB: Single connection with lock (thread-safe, no pool exhaustion) ──
_db_conn      = None
_db_conn_lock = threading.Lock()

def get_db():
    """
    Returns the shared DB connection, reconnecting if needed.
    Uses a lock so only one thread executes a DB call at a time.
    This is safe because DB calls are fast (microseconds).
    """
    global _db_conn
    with _db_conn_lock:
        try:
            if _db_conn is None or _db_conn.closed:
                _db_conn = psycopg2.connect(
                    host=DB_HOST, dbname=DB_NAME, user=DB_USER,
                    password=DB_PASS, port=DB_PORT,
                    cursor_factory=psycopg2.extras.RealDictCursor
                )
            return _db_conn
        except Exception as e:
            _db_conn = psycopg2.connect(
                host=DB_HOST, dbname=DB_NAME, user=DB_USER,
                password=DB_PASS, port=DB_PORT,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            return _db_conn

def db_execute(query, params=None, fetch="none"):
    """
    Thread-safe DB execution with automatic retry.
    fetch: "one", "all", or "none"
    """
    global _db_conn
    for attempt in range(3):
        try:
            with _db_conn_lock:
                if _db_conn is None or _db_conn.closed:
                    _db_conn = psycopg2.connect(
                        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, port=DB_PORT,
                        cursor_factory=psycopg2.extras.RealDictCursor
                    )
                with _db_conn.cursor() as cur:
                    cur.execute(query, params)
                    if fetch == "one":
                        return cur.fetchone()
                    elif fetch == "all":
                        return cur.fetchall()
                    else:
                        _db_conn.commit()
                        return None
        except psycopg2.OperationalError:
            _db_conn = None
            time.sleep(0.1)
        except Exception as e:
            try:
                _db_conn.rollback()
            except Exception:
                _db_conn = None
            if attempt == 2:
                raise
    return None

def release_db(conn):
    pass  # no-op, single connection stays open

def get_tracked_doc_by_doc_id(doc_id):
    return db_execute(
        "SELECT * FROM tracked_docs WHERE doc_id=%s AND is_active=TRUE LIMIT 1",
        (doc_id,), fetch="one")

def get_all_active_docs():
    return db_execute(
        "SELECT * FROM tracked_docs WHERE is_active=TRUE AND status='active'",
        fetch="all") or []

def update_last_change(meeting_id):
    db_execute(
        "UPDATE tracked_docs SET last_change_at=NOW(), status='active', updated_at=NOW() WHERE meeting_id=%s",
        (meeting_id,))

def get_next_version(meeting_id, doc_id):
    row = db_execute(
        "SELECT COALESCE(MAX(version_number),0) AS coalesce FROM doc_snapshots WHERE meeting_id=%s AND doc_id=%s",
        (meeting_id, doc_id), fetch="one")
    return (row["coalesce"] if row and row["coalesce"] is not None else 0) + 1

def save_snapshot(meeting_id, doc_id, version, content, edited_by):
    db_execute(
        "INSERT INTO doc_snapshots (meeting_id,doc_id,version_number,content_text,edited_by) VALUES (%s,%s,%s,%s,%s)",
        (meeting_id, doc_id, version, content, edited_by))

def get_last_snapshot_content(meeting_id, doc_id):
    row = db_execute(
        "SELECT content_text FROM doc_snapshots WHERE meeting_id=%s AND doc_id=%s ORDER BY version_number DESC LIMIT 1",
        (meeting_id, doc_id), fetch="one")
    return row["content_text"] if row else None

def mark_doc_idle_in_db(meeting_id):
    db_execute(
        "UPDATE tracked_docs SET status='idle', updated_at=NOW() WHERE meeting_id=%s",
        (meeting_id,))

def mark_doc_finalized_in_db(meeting_id):
    db_execute(
        "UPDATE tracked_docs SET status='finalized', is_active=FALSE, updated_at=NOW() WHERE meeting_id=%s",
        (meeting_id,))

# ── S3 HELPERS ──
def read_doc_txt(prefix):
    key = f"{prefix}/doc.txt"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8")
    except Exception:
        return ""

def write_doc_txt(prefix, content):
    """Write doc.txt to temp only. Final copy happens at 30-min idle."""
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/doc.txt",
        Body=content.encode("utf-8"),
        ContentType="text/plain"
    )

def strip_current_final_block(doc_txt):
    marker = "\n==================================================\nCURRENT FINAL CONTENT\n"
    idx = doc_txt.find(marker)
    return doc_txt[:idx] if idx != -1 else doc_txt

# ── FIND FINAL S3 PATH ──
def find_final_s3_prefix(meeting_id):
    """
    Search S3 for the final recording folder containing this meeting_id.
    Searches all departments.
    """
    DEPARTMENTS = {
        "Interview-Success": 4,
        "Training":          2,
        "Customer-Success":  2,
        "Marketing":         2,
    }
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
    log.info(f"No final S3 prefix found yet for meeting_id={meeting_id}")
    return None

# ── FINALIZE DOC TO FINAL PATH ──
def finalize_to_final_path(meeting_id, final_prefix):
    temp_prefix = f"temp/live-doc-history/{meeting_id}"
    docs_prefix = f"{final_prefix}/docs"
    src_key     = f"{temp_prefix}/doc.txt"
    dst_key     = f"{docs_prefix}/doc.txt"

    try:
        obj     = s3.get_object(Bucket=S3_BUCKET, Key=src_key)
        content = obj["Body"].read().decode("utf-8")
        content = content.replace(
            f"s3://{S3_BUCKET}/{temp_prefix}/doc.txt",
            f"s3://{S3_BUCKET}/{dst_key}"
        )
        finalized_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        content += (
            f"\n\n==================================================\n"
            f"FINALIZED AT: {finalized_at}\n"
            f"Final S3 Path: s3://{S3_BUCKET}/{dst_key}\n"
        )
        s3.put_object(
            Bucket=S3_BUCKET, Key=dst_key,
            Body=content.encode("utf-8"), ContentType="text/plain"
        )
        log.info(f"doc.txt finalized: s3://{S3_BUCKET}/{dst_key}")
    except Exception as e:
        log.error(f"Failed to copy doc.txt for meeting_id={meeting_id}: {e}")
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
                    Key=f"{docs_prefix}/images/{fname}"
                )
    except Exception:
        pass

    return True

# ── MARK DOC IDLE + FINALIZE ──
def mark_doc_idle(meeting_id):
    log.info(f"30-min idle for meeting_id={meeting_id} — searching final S3 path")
    final_prefix = find_final_s3_prefix(meeting_id)

    if final_prefix:
        success = finalize_to_final_path(meeting_id, final_prefix)
        if success:
            mark_doc_finalized_in_db(meeting_id)
            log.info(f"FINALIZED meeting_id={meeting_id} -> {final_prefix}/docs/doc.txt")
            return

    # Recording not ready — mark idle, retry loop will handle it
    mark_doc_idle_in_db(meeting_id)
    log.info(f"Recording folder not ready for meeting_id={meeting_id} — marked IDLE for retry")

# ── IDLE RETRY LOOP ──
def idle_retry_loop():
    RETRY_INTERVAL = 10 * 60  # 10 min
    while True:
        time.sleep(RETRY_INTERVAL)
        try:
            idle_docs = db_execute(
                "SELECT meeting_id FROM tracked_docs WHERE status='idle' AND is_active=TRUE AND updated_at > NOW() - INTERVAL '6 hours'",
                fetch="all") or []

            if idle_docs:
                log.info(f"Idle retry: checking {len(idle_docs)} idle docs")
            for row in idle_docs:
                mid          = row["meeting_id"]
                final_prefix = find_final_s3_prefix(mid)
                if final_prefix:
                    success = finalize_to_final_path(mid, final_prefix)
                    if success:
                        mark_doc_finalized_in_db(mid)
                        log.info(f"Retry finalized meeting_id={mid}")
        except Exception as e:
            log.error(f"Idle retry loop error: {e}", exc_info=True)

# ── GOOGLE DOC TEXT EXTRACTION ──
def decode_text_safely(raw: bytes) -> str:
    """Permanent Unicode decoder - supports emojis, arrows, all Unicode."""
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def fix_encoding(text: str) -> str:
    """Clean text - strip BOM only. Never use latin-1. Preserves all Unicode."""
    if not text:
        return text
    return text.lstrip("\ufeff")

def extract_text_from_doc(doc_id):
    """
    Returns (plain_text, enriched_images).
    Uses export_media for clean UTF-8 text.
    Falls back to Docs API if export fails (500 error).
    Images only detected when Docs API fallback is used (saves quota).
    """
    drive      = get_drive_service()
    plain_text = ""
    _doc_cache = None  # reused for image detection — avoids extra API call

    try:
        request    = drive.files().export_media(fileId=doc_id, mimeType="text/plain")
        buffer     = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        raw_bytes  = buffer.getvalue()
        plain_text = (decode_text_safely(raw_bytes))
        log.info(f"export_media success for doc_id={doc_id} length={len(plain_text)}")

    except Exception as e:
        log.info(f"export_media using fallback for doc_id={doc_id} ({e})")
        try:
            docs       = get_docs_service()
            _doc_cache = docs.documents().get(documentId=doc_id).execute()
            parts      = []
            for el in _doc_cache.get("body", {}).get("content", []):
                if "paragraph" in el:
                    for pe in el["paragraph"].get("elements", []):
                        if "textRun" in pe:
                            parts.append(pe["textRun"].get("content", ""))
                elif "table" in el:
                    for row in el["table"].get("tableRows", []):
                        for cell in row.get("tableCells", []):
                            for cel in cell.get("content", []):
                                if "paragraph" in cel:
                                    for pe in cel["paragraph"].get("elements", []):
                                        if "textRun" in pe:
                                            parts.append(pe["textRun"].get("content", ""))
            plain_text = fix_encoding("".join(parts))
            log.info(f"Docs API fallback success for doc_id={doc_id} length={len(plain_text)}")
        except Exception as e2:
            log.error(f"Both methods failed for doc_id={doc_id}: {e2}")

    # Images: only when Docs API was already called (saves quota)
    enriched_images = []
    if _doc_cache:
        try:
            inline_objects = _doc_cache.get("inlineObjects", {})
            for el in _doc_cache.get("body", {}).get("content", []):
                if "paragraph" in el:
                    for pe in el["paragraph"].get("elements", []):
                        if "inlineObjectElement" in pe:
                            obj_id = pe["inlineObjectElement"].get("inlineObjectId", "")
                            if obj_id:
                                obj   = inline_objects.get(obj_id, {})
                                props = obj.get("inlineObjectProperties", {}).get("embeddedObject", {})
                                enriched_images.append({
                                    "inline_object_id": obj_id,
                                    "source_uri": props.get("imageProperties", {}).get("sourceUri", ""),
                                    "title": props.get("title", "")
                                })
        except Exception:
            pass

    return plain_text, enriched_images

# ── GET LAST EDITOR ──
def get_last_editor(doc_id):
    try:
        drive     = get_drive_service()
        revisions = drive.revisions().list(
            fileId=doc_id,
            fields="revisions(lastModifyingUser,modifiedTime)"
        ).execute()
        rev_list  = revisions.get("revisions", [])
        if rev_list:
            last = rev_list[-1]
            user = last.get("lastModifyingUser", {})
            return user.get("emailAddress", user.get("displayName", "Unknown"))
    except Exception:
        pass
    return "Unknown"

# ── COMPUTE DIFF ──
def compute_diff(old_text, new_text):
    old_lines = [l for l in old_text.splitlines() if l.strip()]
    new_lines = [l for l in new_text.splitlines() if l.strip()]
    differ    = difflib.unified_diff(old_lines, new_lines, lineterm="")
    added, removed = [], []
    for line in differ:
        if line.startswith("+") and not line.startswith("+++"):
            added.append(line[1:].strip())
        elif line.startswith("-") and not line.startswith("---"):
            removed.append(line[1:].strip())
    return added, removed

# ── PROCESS ONE DOC CHANGE ──
def process_doc_change(doc_record):
    meeting_id = doc_record["meeting_id"]
    doc_id     = doc_record["doc_id"]
    prefix     = doc_record.get("temp_s3_prefix", f"temp/live-doc-history/{meeting_id}")

    log.info(f"Processing change for meeting_id={meeting_id} doc_id={doc_id}")

    # Fetch current doc content via Docs API (no cache delay)
    new_text, _ = extract_text_from_doc(doc_id)
    if not new_text.strip():
        log.info(f"Empty doc content for doc_id={doc_id}, skipping")
        return

    old_text       = get_last_snapshot_content(meeting_id, doc_id) or ""
    added, removed = compute_diff(old_text, new_text)

    if not added and not removed and old_text:
        log.info(f"No text change for doc_id={doc_id}, skipping")
        return

    edited_by     = get_last_editor(doc_id)
    now_utc       = datetime.now(timezone.utc)
    now_ist       = now_utc + IST_OFFSET
    edited_at_str = now_ist.strftime("%Y-%m-%d %I:%M:%S %p IST")
    version       = get_next_version(meeting_id, doc_id)

    version_block  = f"\n==================================================\n"
    version_block += f"VERSION {version}\n"
    version_block += f"Edited At: {edited_at_str}\n"
    version_block += f"Edited By: {edited_by}\n\n"
    version_block += "ADDED:\n"
    if added:
        for line in added:
            version_block += f"- {line}\n"
    else:
        version_block += "- (nothing added)\n"

    version_block += "\nREMOVED:\n"
    if removed:
        for line in removed:
            version_block += f"- {line}\n"
    else:
        version_block += "- (nothing removed)\n"

    version_block += f"\nCURRENT SNAPSHOT:\n{new_text.strip()}\n"

    existing = read_doc_txt(prefix)
    if version == 1 and "TRACKING INITIALIZED" in existing:
        header_end = existing.find("==================================================\nTRACKING INITIALIZED")
        if header_end != -1:
            existing = existing[:header_end]

    stripped    = strip_current_final_block(existing)
    new_doc_txt = stripped + version_block + f"\n==================================================\nCURRENT FINAL CONTENT\n{new_text.strip()}\n"

    write_doc_txt(prefix, new_doc_txt)
    save_snapshot(meeting_id, doc_id, version, new_text, edited_by)
    update_last_change(meeting_id)
    log.info(f"doc.txt updated for meeting_id={meeting_id} version={version} added={len(added)} removed={len(removed)}")

# ── WORKER STATE ──
_active_workers = {}
_workers_lock   = threading.Lock()

def worker_loop(doc_id, meeting_id):
    log.info(f"Worker started for doc_id={doc_id} meeting_id={meeting_id}")
    with _workers_lock:
        _active_workers[doc_id] = {
            "last_activity": time.time(),
            "meeting_id":    meeting_id
        }

    while True:
        with _workers_lock:
            last_activity = _active_workers.get(doc_id, {}).get("last_activity", 0)

        if time.time() - last_activity >= IDLE_TIMEOUT:
            log.info(f"Worker for doc_id={doc_id} idle 30 min — exiting")
            mark_doc_idle(meeting_id)
            with _workers_lock:
                _active_workers.pop(doc_id, None)
            return

        with _workers_lock:
            pending = _active_workers.get(doc_id, {}).get("pending", False)

        if pending:
            with _workers_lock:
                _active_workers[doc_id]["pending"]       = False
                _active_workers[doc_id]["last_activity"] = time.time()
            try:
                doc_record = get_tracked_doc_by_doc_id(doc_id)
                if doc_record and doc_record["is_active"]:
                    # Process immediately
                    process_doc_change(doc_record)
                    # Then poll 3 more times at 30-sec intervals
                    # to catch rapid successive edits Google may batch
                    for _ in range(3):
                        time.sleep(30)
                        with _workers_lock:
                            still_pending = _active_workers.get(doc_id, {}).get("pending", False)
                        if still_pending:
                            break  # new signal arrived, handle normally
                        process_doc_change(doc_record)
                else:
                    log.info(f"doc_id={doc_id} no longer active, worker exiting")
                    with _workers_lock:
                        _active_workers.pop(doc_id, None)
                    return
            except Exception as e:
                log.error(f"Worker error for doc_id={doc_id}: {e}", exc_info=True)

        time.sleep(2)

def signal_or_spawn_worker(executor, doc_id, meeting_id):
    with _workers_lock:
        if doc_id in _active_workers:
            _active_workers[doc_id]["pending"]       = True
            _active_workers[doc_id]["last_activity"] = time.time()
            log.info(f"Signaled existing worker for doc_id={doc_id}")
        else:
            _active_workers[doc_id] = {
                "last_activity": time.time(),
                "meeting_id":    meeting_id,
                "pending":       True
            }
            executor.submit(worker_loop, doc_id, meeting_id)
            log.info(f"Spawned new worker for doc_id={doc_id} meeting_id={meeting_id}")

# ── PARSE SQS MESSAGE ──
def parse_change_message(msg_body, msg_attributes=None):
    channel_id = ""
    doc_id     = ""

    if msg_attributes:
        channel_id = msg_attributes.get("X-Goog-Channel-ID", {}).get("StringValue", "")

    try:
        body       = json.loads(msg_body)
        if "doc_id" in body:
            return body
        channel_id = channel_id or body.get("channelId", body.get("X-Goog-Channel-ID", ""))
        resource_uri = body.get("resourceUri", "")
        if resource_uri:
            parts  = resource_uri.rstrip("/").split("/")
            doc_id = parts[-1] if parts else ""
    except Exception:
        pass  # binary body is normal for Google webhooks

    if not doc_id and channel_id:
        try:
            row = db_execute(
                "SELECT doc_id FROM doc_watch_state WHERE watch_channel_id=%s LIMIT 1",
                (channel_id,), fetch="one")
            if row:
                doc_id = row["doc_id"]
                log.info(f"Resolved doc_id={doc_id} from channel_id={channel_id}")
        except Exception as e:
            log.warning(f"DB channel lookup failed: {e}")

    if doc_id:
        return {"doc_id": doc_id}

    # Fallback: signal all active docs
    log.warning("Cannot identify doc from message — processing all active docs as fallback")
    return {"process_all_active": True}

# ── MAIN ──
def main():
    log.info(f"google_change_worker starting — max_workers={MAX_WORKERS} idle_timeout={IDLE_TIMEOUT}s")

    # Start idle retry background thread
    retry_thread = threading.Thread(target=idle_retry_loop, daemon=True, name="idle-retry")
    retry_thread.start()
    log.info("Idle retry thread started — checks every 10 min for unfinalized idle docs")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="doc-worker") as executor:
        while True:
            try:
                resp = sqs.receive_message(
                    QueueUrl=CHANGE_QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=90,
                    MessageAttributeNames=["All"]
                )
                messages = resp.get("Messages", [])

                for msg in messages:
                    receipt    = msg["ReceiptHandle"]
                    attributes = msg.get("MessageAttributes", {})
                    try:
                        parsed = parse_change_message(msg["Body"], attributes)

                        if not parsed:
                            sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)
                            continue

                        if parsed.get("process_all_active"):
                            active_docs = get_all_active_docs()
                            log.info(f"Fallback: signaling {len(active_docs)} active docs")
                            for doc_record in active_docs:
                                signal_or_spawn_worker(executor, doc_record["doc_id"], doc_record["meeting_id"])
                            sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)
                            continue

                        doc_id     = parsed["doc_id"]
                        doc_record = get_tracked_doc_by_doc_id(doc_id)
                        if not doc_record:
                            log.info(f"doc_id={doc_id} not in tracked_docs, ignoring")
                            sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)
                            continue

                        signal_or_spawn_worker(executor, doc_id, doc_record["meeting_id"])
                        sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)

                    except Exception as e:
                        log.error(f"Error handling SQS message: {e}", exc_info=True)

            except Exception as e:
                log.error(f"Outer loop error: {e}", exc_info=True)
                time.sleep(5)

if __name__ == "__main__":
    main()
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

def find_final_s3_prefix(meeting_id: str) -> str | None:
    """
    Searches ALL department folders in S3 for a folder containing this meeting_id.

    Path structures per department:
      Interview-Success / Host / Year / Month / Candidate / MeetingID / Company / Date / Round / Time /
        => time_offset = 4  (4 folders after meeting_id)

      Training / Customer-Success / Marketing:
      Dept / Host / Year / Month / Candidate / MeetingID / Date / Time /
        => time_offset = 2  (2 folders after meeting_id)
    """
    DEPARTMENTS = {
        "Interview-Success": 4,
        "Training":          2,
        "Customer-Success":  2,
        "Marketing":         2,
    }

    paginator  = s3.get_paginator("list_objects_v2")
    search_str = f"/{meeting_id}/"

    for department, time_offset in DEPARTMENTS.items():
        found_prefixes = set()
        try:
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{department}/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if search_str in key:
                        parts = key.split("/")
                        try:
                            mid_idx    = parts.index(meeting_id)
                            prefix_end = mid_idx + time_offset + 1
                            if len(parts) > prefix_end:
                                time_prefix = "/".join(parts[:prefix_end])
                                found_prefixes.add(time_prefix)
                        except ValueError:
                            continue
            if found_prefixes:
                result = sorted(found_prefixes)[0]
                log.info(f"Found final S3 prefix in [{department}] for meeting_id={meeting_id}: {result}")
                return result
        except Exception as e:
            log.error(f"S3 search error in {department} for meeting_id={meeting_id}: {e}")
            continue

    log.info(f"No final S3 prefix found yet in any department for meeting_id={meeting_id}")
    return None


def finalize_to_final_path(meeting_id: str, final_prefix: str) -> bool:
    """
    Copies temp doc.txt + images + snapshots to final Interview-Success path.
    temp/live-doc-history/<meeting_id>/doc.txt
        → <final_prefix>/docs/doc.txt
    """
    temp_prefix = f"temp/live-doc-history/{meeting_id}"
    docs_prefix = f"{final_prefix}/docs"

    # Copy doc.txt
    src = f"{temp_prefix}/doc.txt"
    dst = f"{docs_prefix}/doc.txt"
    try:
        obj     = s3.get_object(Bucket=S3_BUCKET, Key=src)
        content = obj["Body"].read().decode("utf-8")
        # Update S3 location line inside the file
        content = content.replace(
            f"s3://{S3_BUCKET}/{temp_prefix}/doc.txt",
            f"s3://{S3_BUCKET}/{dst}"
        )
        finalized_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        content += (
            f"\n\n==================================================\n"
            f"FINALIZED AT: {finalized_at}\n"
            f"Final S3 Path: s3://{S3_BUCKET}/{dst}\n"
        )
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=dst,
            Body=content.encode("utf-8"),
            ContentType="text/plain"
        )
        log.info(f"doc.txt → s3://{S3_BUCKET}/{dst}")
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
    except Exception as e:
        log.warning(f"Image copy warning: {e}")

    # Copy snapshots
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{temp_prefix}/snapshots/"):
            for obj in page.get("Contents", []):
                fname = obj["Key"].split("/")[-1]
                s3.copy_object(
                    Bucket=S3_BUCKET,
                    CopySource={"Bucket": S3_BUCKET, "Key": obj["Key"]},
                    Key=f"{docs_prefix}/snapshots/{fname}"
                )
    except Exception as e:
        log.warning(f"Snapshot copy warning: {e}")

    return True


def mark_doc_idle(meeting_id: str):
    """
    Called when worker hits 30-min idle timeout.
    1. Search S3 for final Interview-Success path
    2. Found → finalize immediately
    3. Not found → mark idle, idle_retry_loop will retry every 10 min
    """
    log.info(f"30-min idle for meeting_id={meeting_id} — searching final S3 path")
    final_prefix = find_final_s3_prefix(meeting_id)

    if final_prefix:
        success = finalize_to_final_path(meeting_id, final_prefix)
        if success:
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE tracked_docs SET
                        status = 'finalized',
                        is_active = FALSE,
                        updated_at = NOW()
                    WHERE meeting_id = %s
                """, (meeting_id,))
                conn.commit()
            log.info(f"✅ FINALIZED meeting_id={meeting_id} → {final_prefix}/docs/doc.txt")
            return

    # Recording not ready yet — mark idle for retry
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tracked_docs SET status = 'idle', updated_at = NOW() WHERE meeting_id = %s",
            (meeting_id,)
        )
        conn.commit()
    log.info(f"Recording folder not ready yet for meeting_id={meeting_id} — marked IDLE for retry")

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
    """Write doc.txt to temp prefix. Also write to final path if already known."""
    # Always write to temp
    key = f"{prefix}/doc.txt"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="text/plain"
    )

    # Also write to final Interview-Success path if already found
    try:
        state = read_state_json(prefix)
        final_prefix = state.get("final_s3_prefix", "")
        if final_prefix:
            final_key = f"{final_prefix}/docs/doc.txt"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=final_key,
                Body=content.encode("utf-8"),
                ContentType="text/plain"
            )
            log.info(f"Also wrote doc.txt to final path: {final_key}")
    except Exception as e:
        log.warning(f"Could not write to final path: {e}")


def read_state_json(prefix: str) -> dict:
    """Read state.json from S3 temp prefix."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/state.json")
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return {}


def update_state_json(prefix: str, updates: dict):
    """Update state.json with new fields."""
    try:
        state = read_state_json(prefix)
        state.update(updates)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{prefix}/state.json",
            Body=json.dumps(state, indent=2),
            ContentType="application/json"
        )
    except Exception as e:
        log.warning(f"Could not update state.json: {e}")

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

    # ── Try to find final Interview-Success path if not already known ──
    state = read_state_json(prefix)
    if not state.get("final_s3_prefix"):
        final_prefix = find_final_s3_prefix(meeting_id)
        if final_prefix:
            update_state_json(prefix, {
                "final_s3_prefix": final_prefix,
                "final_doc_txt":   f"s3://{S3_BUCKET}/{final_prefix}/docs/doc.txt",
                "final_found_at":  datetime.now(timezone.utc).isoformat()
            })
            log.info(f"Final path found and saved to state.json: {final_prefix}")

    # Write doc.txt to temp (and final if known)
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
def parse_change_message(msg_body: str, msg_attributes: dict = None) -> dict | None:
    """
    Google webhook → API Gateway → SQS.

    Google Drive notifications send ALL data in HTTP HEADERS, not in the body.
    The body is just a binary notification token (base64-like string).

    API Gateway must be configured to forward these headers into the SQS message.
    We look for channel_id in:
    1. SQS MessageAttributes (if API Gateway forwards headers as attributes)
    2. JSON body fields (if API Gateway maps headers to JSON)
    3. Fallback: query ALL active docs and process all of them (safe fallback)
    """
    channel_id   = ""
    resource_id  = ""
    resource_uri = ""
    doc_id       = ""

    # ── Try SQS MessageAttributes first (most reliable) ──
    if msg_attributes:
        channel_id  = msg_attributes.get("X-Goog-Channel-ID", {}).get("StringValue", "")
        resource_id = msg_attributes.get("X-Goog-Resource-ID", {}).get("StringValue", "")
        resource_uri= msg_attributes.get("X-Goog-Resource-URI", {}).get("StringValue", "")

    # ── Try JSON body ──
    if not channel_id:
        try:
            body       = json.loads(msg_body)
            channel_id = body.get("channelId", body.get("X-Goog-Channel-ID", ""))
            resource_id= body.get("resourceId", body.get("X-Goog-Resource-ID", ""))
            resource_uri=body.get("resourceUri", "")
            if "doc_id" in body:
                return body
        except Exception:
            pass  # body is binary/base64 — that is normal for Google webhooks

    # ── Extract doc_id from resourceUri ──
    if resource_uri:
        parts  = resource_uri.rstrip("/").split("/")
        doc_id = parts[-1] if parts else ""

    # ── Lookup by channel_id in DB ──
    if not doc_id and channel_id:
        try:
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT doc_id FROM doc_watch_state WHERE watch_channel_id = %s LIMIT 1",
                    (channel_id,)
                )
                row = cur.fetchone()
                if row:
                    doc_id = row["doc_id"]
                    log.info(f"Resolved doc_id={doc_id} from channel_id={channel_id}")
        except Exception as e:
            log.warning(f"DB channel lookup failed: {e}")

    if doc_id:
        return {"doc_id": doc_id}

    # ── FALLBACK: Google sent a notification but we cannot identify which doc ──
    # This happens when API Gateway does not forward headers.
    # Safe fallback: return a special signal to process ALL active docs.
    log.warning(f"Cannot identify doc from message — will process all active docs as fallback")
    return {"process_all_active": True}

# ──────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────
def idle_retry_loop():
    """
    Background thread — every 10 min checks 'idle' docs and
    tries to finalize them if the recording S3 folder now exists.
    Gives up after 6 hours.
    """
    RETRY_INTERVAL  = 10 * 60   # 10 minutes
    MAX_RETRY_HOURS = 6

    while True:
        time.sleep(RETRY_INTERVAL)
        try:
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT meeting_id FROM tracked_docs
                    WHERE status = 'idle'
                      AND is_active = TRUE
                      AND updated_at > NOW() - INTERVAL '6 hours'
                """)
                idle_docs = cur.fetchall()
            conn.close()

            if idle_docs:
                log.info(f"Idle retry: checking {len(idle_docs)} idle docs")
            for row in idle_docs:
                mid          = row["meeting_id"]
                final_prefix = find_final_s3_prefix(mid)
                if final_prefix:
                    success = finalize_to_final_path(mid, final_prefix)
                    if success:
                        conn2 = get_db()
                        with conn2.cursor() as cur:
                            cur.execute("""
                                UPDATE tracked_docs SET
                                    status = 'finalized',
                                    is_active = FALSE,
                                    updated_at = NOW()
                                WHERE meeting_id = %s
                            """, (mid,))
                            conn2.commit()
                        conn2.close()
                        log.info(f"✅ Retry finalized meeting_id={mid}")
        except Exception as e:
            log.error(f"Idle retry loop error: {e}", exc_info=True)


def main():
    log.info(f"google_change_worker starting — max_workers={MAX_WORKERS} idle_timeout={IDLE_TIMEOUT_SECONDS}s")

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

                        # ── FALLBACK: process all active docs ──
                        if parsed.get("process_all_active"):
                            active_docs = get_all_active_docs()
                            log.info(f"Fallback: signaling {len(active_docs)} active docs")
                            for doc_record in active_docs:
                                signal_or_spawn_worker(
                                    executor,
                                    doc_record["doc_id"],
                                    doc_record["meeting_id"]
                                )
                            sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)
                            continue

                        # ── NORMAL: specific doc_id identified ──
                        doc_id = parsed["doc_id"]
                        doc_record = get_tracked_doc_by_doc_id(doc_id)
                        if not doc_record:
                            log.info(f"doc_id={doc_id} not in tracked_docs, ignoring")
                            sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)
                            continue

                        meeting_id = doc_record["meeting_id"]
                        signal_or_spawn_worker(executor, doc_id, meeting_id)
                        sqs.delete_message(QueueUrl=CHANGE_QUEUE_URL, ReceiptHandle=receipt)

                    except Exception as e:
                        log.error(f"Error handling SQS message: {e}", exc_info=True)

            except Exception as e:
                log.error(f"Outer loop error: {e}", exc_info=True)
                time.sleep(5)

if __name__ == "__main__":
    main()
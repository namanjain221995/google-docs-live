"""
google_watch_manager.py
-----------------------
Runs as a separate lightweight service.
Every 6 hours:
  - Scans doc_watch_state for watches expiring within 24 hours
  - Stops old channel
  - Registers new files.watch
  - Updates DB

Also handles:
  - Registering watches for newly tracked docs that somehow missed watch registration
"""

import os
import sys
import json
import time
import uuid
import logging
import boto3
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

AWS_REGION         = os.environ.get("AWS_REGION", "us-east-1")
GOOGLE_SECRET_NAME = os.environ.get("GOOGLE_SECRET_NAME", "google/doc-history-service-account")
GOOGLE_WEBHOOK_URL = os.environ.get("GOOGLE_WEBHOOK_URL",
    "https://5cs3mviba8.execute-api.us-east-1.amazonaws.com/google-drive/webhook")
DB_HOST            = os.environ.get("DB_HOST", "localhost")
DB_NAME            = os.environ.get("DB_NAME", "dochistory")
DB_USER            = os.environ.get("DB_USER", "postgres")
DB_PASS            = os.environ.get("DB_PASS", "")
DB_PORT            = int(os.environ.get("DB_PORT", "5432"))

CHECK_INTERVAL_SECONDS = 6 * 3600   # check every 6 hours
RENEW_BEFORE_SECONDS   = 24 * 3600  # renew if expiring within 24 hours

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
]

os.makedirs("/home/ec2-user/doc-ui-worker/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ec2-user/doc-ui-worker/logs/google_watch_manager.log"),
    ]
)
log = logging.getLogger("google_watch_manager")

sm = boto3.client("secretsmanager", region_name=AWS_REGION)

def get_google_creds():
    resp   = sm.get_secret_value(SecretId=GOOGLE_SECRET_NAME)
    secret = json.loads(resp["SecretString"])
    return service_account.Credentials.from_service_account_info(secret, scopes=GOOGLE_SCOPES)

def get_drive_service():
    return build("drive", "v3", credentials=get_google_creds(), cache_discovery=False)

def get_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, port=DB_PORT,
        cursor_factory=psycopg2.extras.RealDictCursor
    )

def stop_watch(drive, channel_id: str, resource_id: str):
    try:
        drive.channels().stop(body={
            "id": channel_id,
            "resourceId": resource_id
        }).execute()
        log.info(f"Stopped old watch channel_id={channel_id}")
    except Exception as e:
        log.warning(f"Could not stop channel_id={channel_id}: {e}")

def register_file_watch(drive, doc_id: str, meeting_id: str) -> dict | None:
    channel_id = str(uuid.uuid4())
    expiry_ms  = int((time.time() + 6 * 24 * 3600) * 1000)
    try:
        response = drive.files().watch(
            fileId=doc_id,
            body={
                "id": channel_id,
                "type": "web_hook",
                "address": GOOGLE_WEBHOOK_URL,
                "expiration": expiry_ms,
                "params": {"meeting_id": meeting_id}
            }
        ).execute()
        return {
            "channel_id": channel_id,
            "resource_id": response.get("resourceId"),
            "expiry_ms": expiry_ms
        }
    except Exception as e:
        log.error(f"Failed to register watch for doc_id={doc_id}: {e}")
        return None

def run_renewal_cycle():
    conn  = get_db()
    drive = get_drive_service()
    now   = datetime.now(timezone.utc)
    threshold = now + timedelta(seconds=RENEW_BEFORE_SECONDS)

    with conn.cursor() as cur:
        # Find watches expiring soon
        cur.execute("""
            SELECT w.*, t.is_active, t.status
            FROM doc_watch_state w
            JOIN tracked_docs t ON t.doc_id = w.doc_id
            WHERE w.watch_expiry < %s
              AND t.is_active = TRUE
              AND t.status = 'active'
        """, (threshold,))
        expiring = cur.fetchall()

    log.info(f"Found {len(expiring)} watches to renew")

    for watch in expiring:
        doc_id     = watch["doc_id"]
        meeting_id = watch["meeting_id"]
        old_ch     = watch["watch_channel_id"]
        old_res    = watch["watch_resource_id"]

        stop_watch(drive, old_ch, old_res)
        new_watch = register_file_watch(drive, doc_id, meeting_id)

        if new_watch:
            expiry_ts = datetime.fromtimestamp(new_watch["expiry_ms"] / 1000, tz=timezone.utc)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE doc_watch_state SET
                        watch_channel_id = %s,
                        watch_resource_id = %s,
                        watch_expiry = %s,
                        updated_at = NOW()
                    WHERE doc_id = %s
                """, (new_watch["channel_id"], new_watch["resource_id"], expiry_ts, doc_id))
            conn.commit()
            log.info(f"Renewed watch for doc_id={doc_id} new_channel={new_watch['channel_id']}")

    # Also register watches for active docs with NO watch entry
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.doc_id, t.meeting_id
            FROM tracked_docs t
            LEFT JOIN doc_watch_state w ON w.doc_id = t.doc_id
            WHERE t.is_active = TRUE AND t.status = 'active'
              AND w.doc_id IS NULL
        """)
        missing = cur.fetchall()

    log.info(f"Found {len(missing)} docs with no watch — registering")
    for row in missing:
        new_watch = register_file_watch(drive, row["doc_id"], row["meeting_id"])
        if new_watch:
            expiry_ts = datetime.fromtimestamp(new_watch["expiry_ms"] / 1000, tz=timezone.utc)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO doc_watch_state
                        (doc_id, meeting_id, watch_channel_id, watch_resource_id, watch_expiry)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (doc_id) DO UPDATE SET
                        watch_channel_id = EXCLUDED.watch_channel_id,
                        watch_resource_id = EXCLUDED.watch_resource_id,
                        watch_expiry = EXCLUDED.watch_expiry,
                        updated_at = NOW()
                """, (row["doc_id"], row["meeting_id"],
                      new_watch["channel_id"], new_watch["resource_id"], expiry_ts))
            conn.commit()
    conn.close()

def main():
    log.info("google_watch_manager starting...")
    while True:
        try:
            run_renewal_cycle()
        except Exception as e:
            log.error(f"Renewal cycle error: {e}", exc_info=True)
        log.info(f"Sleeping {CHECK_INTERVAL_SECONDS}s until next renewal check...")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()

"""
meeting_start_worker.py
-----------------------
Polls zoom-meeting-start-queue.
On meeting.started:
  1. Extracts meeting_id
  2. Looks up Salesforce Interview__c by Zoom_Meeting_Id__c
  3. Reads Google_Docs_ID__c + Google_Docs_URL__c
  4. Inserts row into PostgreSQL tracked_docs
  5. Registers files.watch on the Google Doc (correct per-file scope)
  6. Creates temp S3 state.json and doc.txt
"""

import os
import sys
import json
import time
import logging
import boto3
import psycopg2
import psycopg2.extras
import requests
import uuid
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from simple_salesforce import Salesforce
import base64

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET           = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
MEETING_START_QUEUE = os.environ.get("MEETING_START_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/985100584614/zoom-meeting-start-queue")
GOOGLE_WEBHOOK_URL  = os.environ.get("GOOGLE_WEBHOOK_URL",
    "https://5cs3mviba8.execute-api.us-east-1.amazonaws.com/google-drive/webhook")
SF_SECRET_NAME      = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
GOOGLE_SECRET_NAME  = os.environ.get("GOOGLE_SECRET_NAME", "google/doc-history-service-account")
DB_HOST             = os.environ.get("DB_HOST", "localhost")
DB_NAME             = os.environ.get("DB_NAME", "dochistory")
DB_USER             = os.environ.get("DB_USER", "postgres")
DB_PASS             = os.environ.get("DB_PASS", "")
DB_PORT             = int(os.environ.get("DB_PORT", "5432"))

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents.readonly",
]

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ec2-user/doc-ui-worker/logs/meeting_start_worker.log"),
    ]
)
log = logging.getLogger("meeting_start_worker")

# ──────────────────────────────────────────────
# AWS CLIENTS
# ──────────────────────────────────────────────
sqs    = boto3.client("sqs", region_name=AWS_REGION)
s3     = boto3.client("s3", region_name=AWS_REGION)
sm     = boto3.client("secretsmanager", region_name=AWS_REGION)

# ──────────────────────────────────────────────
# SECRETS
# ──────────────────────────────────────────────
def get_secret(name: str) -> dict:
    resp = sm.get_secret_value(SecretId=name)
    return json.loads(resp["SecretString"])

# ──────────────────────────────────────────────
# DB CONNECTION
# ──────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, port=DB_PORT,
        cursor_factory=psycopg2.extras.RealDictCursor
    )

# ──────────────────────────────────────────────
# SALESFORCE
# ──────────────────────────────────────────────
_sf_client = None

def get_sf():
    global _sf_client
    if _sf_client:
        return _sf_client
    creds = get_secret(SF_SECRET_NAME)
    private_key = base64.b64decode(creds["PRIVATE_KEY_B64"]).decode("utf-8")
    _sf_client = Salesforce(
        username=creds["SF_USERNAME"],
        consumer_key=creds["SF_CLIENT_ID"],
        privatekey=private_key,
        domain="login"
    )
    return _sf_client

def lookup_salesforce(meeting_id: str) -> dict | None:
    sf = get_sf()
    query = f"""
        SELECT Id, Name, Zoom_Meeting_Id__c,
               Google_Docs_ID__c, Google_Docs_URL__c,
               Candidate_Name__c, Company__c, Interviewer_s_Name__c
        FROM Interview__c
        WHERE Zoom_Meeting_Id__c = '{meeting_id}'
        LIMIT 1
    """
    result = sf.query(query)
    records = result.get("records", [])
    if not records:
        log.warning(f"No Salesforce record found for meeting_id={meeting_id}")
        return None
    return records[0]

# ──────────────────────────────────────────────
# GOOGLE AUTH
# ──────────────────────────────────────────────
_google_creds = None

def get_google_creds():
    global _google_creds
    if _google_creds:
        return _google_creds
    secret = get_secret(GOOGLE_SECRET_NAME)
    _google_creds = service_account.Credentials.from_service_account_info(
        secret, scopes=GOOGLE_SCOPES
    )
    return _google_creds

def get_drive_service():
    return build("drive", "v3", credentials=get_google_creds(), cache_discovery=False)

# ──────────────────────────────────────────────
# GOOGLE FILES.WATCH (correct per-file scope)
# ──────────────────────────────────────────────
def register_file_watch(doc_id: str, meeting_id: str) -> dict | None:
    """
    Register a files.watch on a specific Google Doc file.
    This is the CORRECT approach — not changes.watch which has scope issues.
    Each doc gets its own watch channel.
    Watch expires in 7 days max (Google limit). We renew via watch_manager.
    """
    drive = get_drive_service()
    channel_id = str(uuid.uuid4())
    expiry_ms = int((time.time() + 6 * 24 * 3600) * 1000)  # 6 days, before 7-day Google limit

    body = {
        "id": channel_id,
        "type": "web_hook",
        "address": GOOGLE_WEBHOOK_URL,
        "expiration": expiry_ms,
        "params": {
            "meeting_id": meeting_id
        }
    }

    try:
        response = drive.files().watch(fileId=doc_id, body=body).execute()
        log.info(f"Watch registered for doc_id={doc_id} meeting_id={meeting_id} channel={channel_id}")
        return {
            "channel_id": channel_id,
            "resource_id": response.get("resourceId"),
            "expiry_ms": expiry_ms
        }
    except Exception as e:
        log.error(f"Failed to register watch for doc_id={doc_id}: {e}")
        return None

# ──────────────────────────────────────────────
# S3 HELPERS
# ──────────────────────────────────────────────
def create_temp_s3_state(meeting_id: str, sf_record: dict, doc_id: str, doc_url: str):
    prefix = f"temp/live-doc-history/{meeting_id}"

    # state.json
    candidate  = sf_record.get("Candidate_Name__c", "Unknown")
    company    = sf_record.get("Company__c", "Unknown")
    host_name  = sf_record.get("Interviewer_s_Name__c", "Unknown")
    now_utc    = datetime.now(timezone.utc)

    state = {
        "meeting_id":           meeting_id,
        "doc_id":               doc_id,
        "doc_url":              doc_url,
        "salesforce_record_id": sf_record.get("Id"),
        "candidate":            candidate,
        "company":              company,
        "host_name":            host_name,
        "temp_s3_prefix":       f"temp/live-doc-history/{meeting_id}",
        "final_s3_prefix":      "",
        "final_doc_txt":        "",
        "status":               "active",
        "initialized_at":       now_utc.isoformat(),
        "initialized_at_ist":   (now_utc + __import__("datetime").timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %I:%M:%S %p IST"),
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/state.json",
        Body=json.dumps(state, indent=2),
        ContentType="application/json"
    )

    # Initial doc.txt
    candidate  = sf_record.get("Candidate_Name__c", "Unknown")
    company    = sf_record.get("Company__c", "Unknown")
    s3_loc     = f"s3://{S3_BUCKET}/{prefix}/doc.txt"
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    doc_txt = f"""DOCUMENT VERSION HISTORY
Document ID: {doc_id}
Document URL: {doc_url}
Meeting ID: {meeting_id}
Candidate: {candidate}
Company: {company}
S3 Location: {s3_loc}
Tracking Started: {now_str}

==================================================
TRACKING INITIALIZED
No changes captured yet. Live tracking is active.
==================================================

CURRENT FINAL CONTENT
<not captured yet - will populate on first Google Doc edit>
"""
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/doc.txt",
        Body=doc_txt,
        ContentType="text/plain"
    )
    log.info(f"Temp S3 state created: s3://{S3_BUCKET}/{prefix}/")
    return prefix

# ──────────────────────────────────────────────
# DB HELPERS
# ──────────────────────────────────────────────
def upsert_tracked_doc(conn, meeting_id, doc_id, doc_url, sf_record, temp_prefix, watch_info):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tracked_docs
                (meeting_id, doc_id, doc_url, salesforce_record_id,
                 candidate, company, host_name, temp_s3_prefix, status, is_active, last_change_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active', TRUE, NOW())
            ON CONFLICT (meeting_id) DO UPDATE SET
                doc_id = EXCLUDED.doc_id,
                doc_url = EXCLUDED.doc_url,
                salesforce_record_id = EXCLUDED.salesforce_record_id,
                candidate = EXCLUDED.candidate,
                company = EXCLUDED.company,
                host_name = EXCLUDED.host_name,
                temp_s3_prefix = EXCLUDED.temp_s3_prefix,
                status = 'active',
                is_active = TRUE,
                last_change_at = NOW(),
                updated_at = NOW()
        """, (
            meeting_id, doc_id, doc_url,
            sf_record.get("Id"),
            sf_record.get("Candidate_Name__c", "Unknown"),
            sf_record.get("Company__c", "Unknown"),
            sf_record.get("Interviewer_s_Name__c", "Unknown"),
            temp_prefix
        ))

        if watch_info:
            expiry_ts = datetime.fromtimestamp(watch_info["expiry_ms"] / 1000, tz=timezone.utc)
            cur.execute("""
                INSERT INTO doc_watch_state
                    (doc_id, meeting_id, watch_channel_id, watch_resource_id, watch_expiry)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (doc_id) DO UPDATE SET
                    meeting_id = EXCLUDED.meeting_id,
                    watch_channel_id = EXCLUDED.watch_channel_id,
                    watch_resource_id = EXCLUDED.watch_resource_id,
                    watch_expiry = EXCLUDED.watch_expiry,
                    updated_at = NOW()
            """, (
                doc_id, meeting_id,
                watch_info["channel_id"],
                watch_info["resource_id"],
                expiry_ts
            ))
        conn.commit()

# ──────────────────────────────────────────────
# PROCESS ONE MEETING.STARTED MESSAGE
# ──────────────────────────────────────────────
def process_message(body: dict, conn):
    # Zoom webhook sends: {event, account_id, object: {id, topic, ...}}
    # Try direct object.id first (current Zoom format)
    meeting_obj = body.get("object", {})
    meeting_id  = str(meeting_obj.get("id", ""))

    # Fallback: old payload wrapper format
    if not meeting_id:
        payload     = body.get("payload", {})
        meeting_obj = payload.get("object", {})
        meeting_id  = str(meeting_obj.get("id", ""))

    if not meeting_id:
        log.warning(f"No meeting_id found in message body: {body}")
        return

    log.info(f"Processing meeting.started for meeting_id={meeting_id}")

    # Salesforce lookup
    sf_record = lookup_salesforce(meeting_id)
    if not sf_record:
        log.warning(f"Skipping meeting_id={meeting_id} — no Salesforce match")
        return

    doc_id  = sf_record.get("Google_Docs_ID__c", "")
    doc_url = sf_record.get("Google_Docs_URL__c", "")

    if not doc_id:
        log.warning(f"No Google_Docs_ID__c for meeting_id={meeting_id}, skipping doc tracking")
        return

    log.info(f"Found doc_id={doc_id} for meeting_id={meeting_id}")

    # Register per-file Google watch
    watch_info = register_file_watch(doc_id, meeting_id)

    # Create temp S3 state
    temp_prefix = create_temp_s3_state(meeting_id, sf_record, doc_id, doc_url)

    # Insert into PostgreSQL
    upsert_tracked_doc(conn, meeting_id, doc_id, doc_url, sf_record, temp_prefix, watch_info)

    log.info(f"Successfully initialized doc tracking for meeting_id={meeting_id} doc_id={doc_id}")

# ──────────────────────────────────────────────
# MAIN POLL LOOP
# ──────────────────────────────────────────────
def main():
    log.info("meeting_start_worker starting...")
    conn = get_db()

    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=MEETING_START_QUEUE,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,    # long polling
                VisibilityTimeout=60
            )
            messages = resp.get("Messages", [])
            if not messages:
                continue

            for msg in messages:
                receipt = msg["ReceiptHandle"]
                try:
                    outer = json.loads(msg["Body"])
                    # SQS from SNS wraps body in Message field
                    if "Message" in outer:
                        body = json.loads(outer["Message"])
                    else:
                        body = outer

                    process_message(body, conn)
                    sqs.delete_message(QueueUrl=MEETING_START_QUEUE, ReceiptHandle=receipt)

                except Exception as e:
                    log.error(f"Error processing message: {e}", exc_info=True)
                    # Don't delete — let it retry

        except psycopg2.InterfaceError:
            log.warning("DB connection lost, reconnecting...")
            conn = get_db()

        except Exception as e:
            log.error(f"Outer loop error: {e}", exc_info=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
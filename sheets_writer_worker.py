"""
sheets_writer_worker.py
-----------------------
Triggered after llm_processor_worker creates llm-done.json.

Flow per meeting:
1. Scan temp/live-doc-history/ for llm-done.json WITHOUT sheets-done.json
2. Read llm-done.json → get meeting_id + base_prefix (final S3 path)
3. Find llm.txt in final S3 path: <base_prefix>/llm/llm.txt
4. Parse LLM JSON output
5. Query Salesforce by Zoom Meeting ID → get Interview ID, candidate, date
6. Extract IS person + year/month from base_prefix
7. Navigate/create Google Drive: Interview Success → Year → Month → Sheet
8. Write rows to correct tabs based on routing flags
9. Write sheets-done.json to temp to mark done

Key fixes:
- llm.txt is at Interview-Success/.../llm/llm.txt (NOT in temp)
- base_prefix comes from llm-done.json OR done.json
- Per-key locking prevents duplicate folder creation
- Rate limiter: 45 writes/min max to avoid 429
- Exponential backoff retry on 429
- Correct S3 path parsing for temp structure
"""

import os
import sys
import json
import time
import logging
import threading
import queue
import base64
import re
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from simple_salesforce import Salesforce

# ── CONFIG ───────────────────────────────────────────────────────────────────

AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET        = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
SF_SECRET_NAME   = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
API_SECRET_NAME  = os.environ.get("API_SECRET_NAME", "secrets/api")

SHARED_DRIVE_NAME = "2026_Shared_Drive"
GDRIVE_FOLDER     = "Interview Success"

LIVE_WORKERS         = 10
BACKFILL_WORKERS     = 10
LIVE_POLL_INTERVAL   = 30    # seconds
BACKFILL_POLL_INTERVAL = 120 # seconds

IST_OFFSET = timedelta(hours=5, minutes=30)

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Departments to search for llm.txt when base_prefix is unknown
DEPARTMENTS = ["Interview-Success", "Training", "Customer-Success", "Marketing"]

# ── LOGGING ──────────────────────────────────────────────────────────────────

os.makedirs("/home/ec2-user/google-docs-live/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            "/home/ec2-user/google-docs-live/logs/sheets_writer_worker.log"
        ),
    ],
)
log = logging.getLogger("sheets_writer")

# ── RATE LIMITER (45 writes/min across all threads) ───────────────────────────

_RATE_LIMIT  = 45
_RATE_WINDOW = 60
_rate_lock   = threading.Lock()
_rate_times  = []

def _acquire_write_token():
    while True:
        with _rate_lock:
            now    = time.time()
            cutoff = now - _RATE_WINDOW
            global _rate_times
            _rate_times = [t for t in _rate_times if t > cutoff]
            if len(_rate_times) < _RATE_LIMIT:
                _rate_times.append(now)
                return
            wait = _rate_times[0] + _RATE_WINDOW - now + 0.1
        time.sleep(max(wait, 0.1))

# ── LIVE QUEUE ────────────────────────────────────────────────────────────────

live_queue  = queue.Queue(maxsize=500)
_seen       = set()
_seen_lock  = threading.Lock()

# ── AWS CLIENTS ───────────────────────────────────────────────────────────────

_boto_cfg = Config(max_pool_connections=100)
s3  = boto3.client("s3",             region_name=AWS_REGION, config=_boto_cfg)
sm  = boto3.client("secretsmanager", region_name=AWS_REGION)

# ── SECRETS ───────────────────────────────────────────────────────────────────

_secrets_cache = {}
_secrets_lock  = threading.Lock()

def get_secret(name: str) -> dict:
    with _secrets_lock:
        if name in _secrets_cache:
            return _secrets_cache[name]
        raw = sm.get_secret_value(SecretId=name)["SecretString"]
        parsed = json.loads(raw)
        _secrets_cache[name] = parsed
        return parsed

# ── GOOGLE AUTH ───────────────────────────────────────────────────────────────

_google_creds      = None
_google_creds_lock = threading.Lock()

def get_google_creds():
    global _google_creds
    with _google_creds_lock:
        if _google_creds:
            return _google_creds
        secret = get_secret(API_SECRET_NAME)
        sa_raw = secret.get("service-account", secret)
        sa_info = json.loads(sa_raw) if isinstance(sa_raw, str) else sa_raw
        _google_creds = service_account.Credentials.from_service_account_info(
            sa_info, scopes=GOOGLE_SCOPES
        )
        return _google_creds

def get_drive_service():
    return build("drive",  "v3", credentials=get_google_creds(), cache_discovery=False)

def get_sheets_service():
    return build("sheets", "v4", credentials=get_google_creds(), cache_discovery=False)

# ── SALESFORCE ────────────────────────────────────────────────────────────────

_sf_client = None
_sf_lock   = threading.Lock()

def get_sf():
    global _sf_client
    with _sf_lock:
        if _sf_client:
            return _sf_client
        creds = get_secret(SF_SECRET_NAME)
        private_key = base64.b64decode(creds["PRIVATE_KEY_B64"]).decode("utf-8")
        _sf_client = Salesforce(
            username=creds["SF_USERNAME"],
            consumer_key=creds["SF_CLIENT_ID"],
            privatekey=private_key,
            domain="login",
        )
        return _sf_client

def query_sf_by_meeting_id(meeting_id: str) -> dict:
    try:
        sf = get_sf()
        result = sf.query(f"""
            SELECT Id, Name, Zoom_Meeting_Id__c,
                   Candidate_Name__c, Company__c,
                   Interviewer_s_Name__c, Recruiter_Name__c,
                   Date_of_Interview__c, Round_Info__c, Round__c
            FROM Interview__c
            WHERE Zoom_Meeting_Id__c = '{meeting_id}'
            LIMIT 1
        """)
        records = result.get("records", [])
        if not records:
            log.warning(f"[{meeting_id}] No SF record found")
            return {}
        r = records[0]
        return {
            "sf_interview_id":   r.get("Name", ""),
            "candidate_name":    r.get("Candidate_Name__c", ""),
            "company":           r.get("Company__c", ""),
            "date_of_interview": r.get("Date_of_Interview__c", ""),
            "round_info":        r.get("Round_Info__c", r.get("Round__c", "")),
            "interviewer_name":  r.get("Interviewer_s_Name__c", ""),
        }
    except Exception as e:
        log.error(f"[{meeting_id}] Salesforce error: {e}")
        return {}

# ── S3 HELPERS ────────────────────────────────────────────────────────────────

def read_s3_text(key: str) -> str:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8")
    except Exception:
        return ""

def write_s3_json(key: str, data: dict):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

def find_llm_txt_in_s3(meeting_id: str, base_prefix: str = "") -> tuple:
    """
    Find llm.txt for a meeting.
    Returns (s3_key, base_prefix) or ("", "")

    Strategy:
    1. If base_prefix known → try <base_prefix>/llm/llm.txt directly
    2. Search Interview-Success/<host>/.../<meeting_id>/llm/llm.txt
    """
    # Strategy 1: direct from base_prefix
    if base_prefix:
        key = f"{base_prefix}/llm/llm.txt"
        txt = read_s3_text(key)
        if txt:
            return key, base_prefix

    # Strategy 2: scan Interview-Success/ for the meeting_id folder
    paginator = s3.get_paginator("list_objects_v2")
    for dept in DEPARTMENTS:
        prefix = f"{dept}/"
        try:
            for page in paginator.paginate(
                Bucket=S3_BUCKET,
                Prefix=prefix,
                Delimiter="/"
            ):
                # We need to go deeper — search for meeting_id/llm/llm.txt
                pass
        except Exception:
            continue

    # Broader scan — list all llm.txt files under Interview-Success containing meeting_id
    try:
        for dept in DEPARTMENTS:
            resp = s3.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=f"{dept}/",
            )
            # This is too broad — use a targeted search instead
            break
    except Exception:
        pass

    # Most reliable: scan temp structure to find done.json which has base_prefix
    return "", ""

def find_base_prefix_for_meeting(meeting_id: str) -> str:
    """
    Scan Interview-Success/ (and other depts) to find the folder
    that contains meeting_id in its path and has llm/llm.txt.
    Returns the base prefix (up to and including meeting_id) or "".
    """
    paginator = s3.get_paginator("list_objects_v2")
    for dept in DEPARTMENTS:
        try:
            for page in paginator.paginate(
                Bucket=S3_BUCKET,
                Prefix=f"{dept}/",
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if meeting_id in key and key.endswith("/llm/llm.txt"):
                        # Extract base_prefix = everything up to /<meeting_id>
                        idx = key.find(f"/{meeting_id}/")
                        if idx != -1:
                            return key[:idx + len(f"/{meeting_id}")]
        except Exception as e:
            log.warning(f"Scan error for {dept}: {e}")
            continue
    return ""

# ── S3 SCANNER (temp/) ────────────────────────────────────────────────────────

def scan_s3_for_unprocessed(since_modified=None) -> list:
    """
    Scan temp/live-doc-history/ for meetings that have:
      - llm-done.json   ✅
      - sheets-done.json ❌ (not yet processed)

    Handles both path structures:
      NEW: temp/live-doc-history/2026/Month-5/2026-05-01/<meeting_id>/file
      OLD: temp/live-doc-history/<meeting_id>/file
    """
    paginator = s3.get_paginator("list_objects_v2")
    has_llm_done    = {}   # meeting_id → {prefix, last_modified, key}
    has_sheets_done = set()

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix="temp/live-doc-history/"
    ):
        for obj in page.get("Contents", []):
            key           = obj["Key"]
            last_modified = obj.get("LastModified")

            # Remove the leading "temp/live-doc-history/" prefix
            # key format: temp/live-doc-history/....
            parts = key.split("/")
            # parts[0] = "temp"
            # parts[1] = "live-doc-history"
            # NEW: parts[2]=YYYY, parts[3]=Month-M, parts[4]=YYYY-MM-DD, parts[5]=meeting_id, parts[6]=file
            # OLD: parts[2]=meeting_id, parts[3]=file

            if len(parts) < 4:
                continue

            # Detect NEW structure: parts[2] is 4-digit year, parts[3] starts with Month-
            if (parts[2].isdigit() and len(parts[2]) == 4
                    and parts[3].startswith("Month-")
                    and len(parts) >= 7):
                meeting_id = parts[5]
                filename   = parts[6]
                prefix     = "/".join(parts[:6])   # temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/meeting_id

            # OLD structure: parts[2] is meeting_id (all digits)
            elif parts[2].isdigit() and len(parts) == 4:
                meeting_id = parts[2]
                filename   = parts[3]
                prefix     = "/".join(parts[:3])   # temp/live-doc-history/meeting_id

            else:
                continue

            if not meeting_id.isdigit():
                continue

            if filename == "llm-done.json":
                if since_modified is None or (last_modified and last_modified > since_modified):
                    has_llm_done[meeting_id] = {
                        "prefix":        prefix,
                        "last_modified": last_modified,
                        "key":           key,
                    }
            elif filename == "sheets-done.json":
                has_sheets_done.add(meeting_id)

    # Return meetings that need processing — newest first
    pending = []
    for mid, info in has_llm_done.items():
        if mid not in has_sheets_done:
            pending.append({
                "meeting_id":    mid,
                "temp_prefix":   info["prefix"],
                "llm_done_key":  info["key"],
                "last_modified": info["last_modified"],
            })

    pending.sort(
        key=lambda x: x["last_modified"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    log.info(f"Scanner found {len(pending)} meetings to process "
             f"({len(has_llm_done)} have llm-done, {len(has_sheets_done)} already have sheets-done)")
    return pending

# ── LLM OUTPUT PARSER ─────────────────────────────────────────────────────────

def parse_llm_output(llm_txt: str) -> dict:
    """
    Parse llm.txt which contains a header then JSON.
    Strips header (everything before first '{') and parses JSON.
    """
    json_start = llm_txt.find("{")
    if json_start == -1:
        log.warning("No JSON found in llm.txt")
        return {}
    try:
        data = json.loads(llm_txt[json_start:])
    except json.JSONDecodeError:
        try:
            decoder = json.JSONDecoder()
            data, _ = decoder.raw_decode(llm_txt[json_start:])
        except Exception as e:
            log.error(f"JSON parse error: {e}")
            return {}

    summary_card   = data.get("audit_summary_card", {})
    metadata       = data.get("audit_metadata", {})
    candidate_perf = data.get("overall_candidate_performance", {})
    proxy_perf     = data.get("overall_proxy_support_performance", {})
    verdict        = data.get("final_verdict", {})

    def join_categories(cats):
        if isinstance(cats, list):
            return "; ".join(
                item.get("category", str(item)) if isinstance(item, dict) else str(item)
                for item in cats
            )
        return str(cats) if cats else ""

    return {
        "candidate_name":                  (summary_card.get("candidate_name")
                                            or metadata.get("candidate_detected", "")),
        "chance_of_moving_to_next_round":  summary_card.get("chance_of_moving_to_next_round_percent", ""),
        "candidate_action_required":       bool(summary_card.get("candidate_action_required", False)),
        "proxy_support_action_required":   bool(summary_card.get("proxy_support_action_required", False)),
        "candidate_action_categories":     join_categories(summary_card.get("candidate_action_categories", [])),
        "proxy_support_action_categories": join_categories(summary_card.get("proxy_support_action_categories", [])),
        "candidate_score":                 str(candidate_perf.get("score", "")),
        "proxy_score":                     str(proxy_perf.get("score", "")),
        "verdict":                         verdict.get("one_line_verdict", ""),
        "round_type":                      metadata.get("round_type_detected", ""),
    }

# ── PATH HELPERS ──────────────────────────────────────────────────────────────

def clean_name(raw: str) -> str:
    return raw.replace("_", " ").strip()

def extract_is_person(base_prefix: str) -> str:
    """
    base_prefix: Interview-Success/Ronak_Thakar/2026/...
    → 'Ronak Thakar'
    """
    parts = base_prefix.split("/")
    if len(parts) >= 2 and parts[0] in DEPARTMENTS:
        return clean_name(parts[1])
    return ""

def extract_year_month(base_prefix: str):
    """
    Returns (year_str, month_num, month_name)
    from path like Interview-Success/Host/2026/Month-5/...
    """
    parts = base_prefix.split("/")
    year = ""
    month_folder = ""
    for p in parts:
        if p.isdigit() and len(p) == 4:
            year = p
        elif p.startswith("Month-"):
            month_folder = p
    try:
        month_num = int(month_folder.replace("Month-", ""))
    except Exception:
        month_num = 0
    month_names = ["", "January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    month_name = month_names[month_num] if 0 < month_num <= 12 else "Unknown"
    return year, month_num, month_name

# ── GOOGLE DRIVE: FOLDER/SHEET CREATE WITH RACE PROTECTION ───────────────────

_drive_cache         = {}
_drive_cache_lock    = threading.Lock()
_create_locks        = {}
_create_locks_lock   = threading.Lock()

def _get_create_lock(key: str) -> threading.Lock:
    with _create_locks_lock:
        if key not in _create_locks:
            _create_locks[key] = threading.Lock()
        return _create_locks[key]

def find_shared_drive_id(drive_svc) -> str:
    cache_key = "__shared_drive__"
    with _drive_cache_lock:
        if cache_key in _drive_cache:
            return _drive_cache[cache_key]

    with _get_create_lock(cache_key):
        with _drive_cache_lock:
            if cache_key in _drive_cache:
                return _drive_cache[cache_key]
        resp = drive_svc.drives().list(pageSize=20).execute()
        for d in resp.get("drives", []):
            if d["name"] == SHARED_DRIVE_NAME:
                with _drive_cache_lock:
                    _drive_cache[cache_key] = d["id"]
                return d["id"]
        raise ValueError(f"Shared drive '{SHARED_DRIVE_NAME}' not found")

def find_or_create_folder(drive_svc, name: str, parent_id: str, drive_id: str) -> str:
    """
    Find or create folder. Uses per-key lock + double-check to prevent
    duplicate folder creation when multiple threads run at startup.
    """
    cache_key = f"folder:{parent_id}:{name}"

    # Fast path
    with _drive_cache_lock:
        if cache_key in _drive_cache:
            return _drive_cache[cache_key]

    # Slow path — only one thread per folder key
    with _get_create_lock(cache_key):
        # Double-check after lock
        with _drive_cache_lock:
            if cache_key in _drive_cache:
                return _drive_cache[cache_key]

        # Search Drive
        q = (
            f"name='{name}' "
            f"and mimeType='application/vnd.google-apps.folder' "
            f"and '{parent_id}' in parents "
            f"and trashed=false"
        )
        resp = drive_svc.files().list(
            q=q,
            spaces="drive",
            fields="files(id,name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            corpora="drive",
            driveId=drive_id,
        ).execute()

        files = resp.get("files", [])
        if files:
            fid = files[0]["id"]
            if len(files) > 1:
                log.warning(f"Found {len(files)} folders named '{name}' — using first ({fid})")
        else:
            f = drive_svc.files().create(
                body={
                    "name": name,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent_id],
                },
                fields="id",
                supportsAllDrives=True,
            ).execute()
            fid = f["id"]
            log.info(f"✅ Created folder '{name}' → {fid}")

        with _drive_cache_lock:
            _drive_cache[cache_key] = fid
        return fid

def find_or_create_sheet(drive_svc, sheets_svc, name: str, parent_id: str, drive_id: str) -> str:
    cache_key = f"sheet:{parent_id}:{name}"

    with _drive_cache_lock:
        if cache_key in _drive_cache:
            return _drive_cache[cache_key]

    with _get_create_lock(cache_key):
        with _drive_cache_lock:
            if cache_key in _drive_cache:
                return _drive_cache[cache_key]

        q = (
            f"name='{name}' "
            f"and mimeType='application/vnd.google-apps.spreadsheet' "
            f"and '{parent_id}' in parents "
            f"and trashed=false"
        )
        resp = drive_svc.files().list(
            q=q,
            spaces="drive",
            fields="files(id,name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            corpora="drive",
            driveId=drive_id,
        ).execute()

        files = resp.get("files", [])
        if files:
            sid = files[0]["id"]
            log.info(f"Found existing sheet '{name}': {sid}")
        else:
            f = drive_svc.files().create(
                body={
                    "name": name,
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "parents": [parent_id],
                },
                fields="id",
                supportsAllDrives=True,
            ).execute()
            sid = f["id"]
            log.info(f"✅ Created sheet '{name}': {sid}")
            setup_sheet_tabs(sheets_svc, sid)

        with _drive_cache_lock:
            _drive_cache[cache_key] = sid
        return sid

# ── SHEET TAB SETUP ───────────────────────────────────────────────────────────

CANDIDATE_HEADERS = [
    "Date", "Salesforce Interview ID", "Candidate Name", "Meeting ID",
    "Chance of Moving to Next Round %", "Action Required",
    "Candidate Action Categories",
]
IS_HEADERS = [
    "Date", "Salesforce Interview ID", "Interview-Success Person", "Meeting ID",
    "Chance of Moving to Next Round %", "Action Required",
    "Proxy Support Action Categories",
]
DATA_HEADERS = [
    "Date", "Salesforce Interview ID", "Candidate Name", "Interview-Success Person",
    "Meeting ID", "Chance of Moving to Next Round %",
    "Candidate Action Required", "Proxy Support Action Required",
    "Candidate Action Categories", "Proxy Support Action Categories",
    "Candidate Score", "Proxy Score", "Verdict", "Round Type",
]

def setup_sheet_tabs(sheets_svc, spreadsheet_id: str):
    meta     = sheets_svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = meta.get("sheets", [])
    existing_titles = [s["properties"]["title"] for s in existing]

    requests = []
    if existing:
        first_id = existing[0]["properties"]["sheetId"]
        if existing[0]["properties"]["title"] != "Candidate":
            requests.append({
                "updateSheetProperties": {
                    "properties": {"sheetId": first_id, "title": "Candidate"},
                    "fields": "title",
                }
            })
    for tab in ["Interview-Success", "Data"]:
        if tab not in existing_titles:
            requests.append({"addSheet": {"properties": {"title": tab}}})

    if requests:
        sheets_svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    sheets_svc.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "RAW",
            "data": [
                {"range": "'Candidate'!A1",         "values": [CANDIDATE_HEADERS]},
                {"range": "'Interview-Success'!A1", "values": [IS_HEADERS]},
                {"range": "'Data'!A1",              "values": [DATA_HEADERS]},
            ],
        },
    ).execute()
    log.info(f"Sheet tabs set up for {spreadsheet_id}")

# ── APPEND ROW (rate-limited + retry) ────────────────────────────────────────

def append_row(sheets_svc, spreadsheet_id: str, tab: str, row: list):
    max_retries = 5
    for attempt in range(max_retries):
        _acquire_write_token()
        try:
            sheets_svc.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            ).execute()
            return
        except HttpError as e:
            if e.resp.status == 429:
                wait = (2 ** attempt) * 5
                log.warning(f"429 on tab={tab} attempt {attempt+1}/{max_retries} — sleep {wait}s")
                time.sleep(wait)
                if attempt == max_retries - 1:
                    raise
            else:
                log.error(f"Sheets error on tab={tab}: {e}")
                raise

# ── CORE: PROCESS ONE MEETING ─────────────────────────────────────────────────

def process_one_meeting(item: dict) -> str:
    meeting_id  = item["meeting_id"]
    temp_prefix = item["temp_prefix"]

    log.info(f"[{meeting_id}] Processing...")

    # ── Step 1: Read llm-done.json ─────────────────────────────────────────
    llm_done_raw = read_s3_text(f"{temp_prefix}/llm-done.json")
    if not llm_done_raw:
        return f"SKIP {meeting_id} — llm-done.json empty"
    try:
        llm_done = json.loads(llm_done_raw)
    except Exception:
        return f"SKIP {meeting_id} — llm-done.json parse error"

    # base_prefix may be stored directly in llm-done.json
    base_prefix = llm_done.get("base_prefix", "")
    log.info(f"[{meeting_id}] base_prefix from llm-done.json: '{base_prefix}'")

    # ── Step 2: If no base_prefix, read done.json ──────────────────────────
    if not base_prefix:
        done_raw = read_s3_text(f"{temp_prefix}/done.json")
        if done_raw:
            try:
                done_data   = json.loads(done_raw)
                base_prefix = done_data.get("base_prefix", "")
                log.info(f"[{meeting_id}] base_prefix from done.json: '{base_prefix}'")
            except Exception:
                pass

    # ── Step 3: Find llm.txt ───────────────────────────────────────────────
    llm_txt  = ""
    llm_key  = ""

    # Try direct path first: base_prefix/llm/llm.txt
    if base_prefix:
        # Strip s3://bucket/ prefix if present
        bp = base_prefix.replace(f"s3://{S3_BUCKET}/", "").rstrip("/")
        candidate_key = f"{bp}/llm/llm.txt"
        llm_txt = read_s3_text(candidate_key)
        if llm_txt:
            llm_key     = candidate_key
            base_prefix = bp
            log.info(f"[{meeting_id}] Found llm.txt at: {llm_key}")

    # Fallback: scan all departments for meeting_id/llm/llm.txt
    if not llm_txt:
        log.info(f"[{meeting_id}] Scanning S3 for llm.txt...")
        found_prefix = find_base_prefix_for_meeting(meeting_id)
        if found_prefix:
            base_prefix = found_prefix
            llm_key     = f"{base_prefix}/llm/llm.txt"
            llm_txt     = read_s3_text(llm_key)
            if llm_txt:
                log.info(f"[{meeting_id}] Found llm.txt via scan: {llm_key}")

    if not llm_txt:
        return f"SKIP {meeting_id} — llm.txt not found (base_prefix='{base_prefix}')"

    # ── Step 4: Parse LLM output ───────────────────────────────────────────
    parsed = parse_llm_output(llm_txt)
    if not parsed:
        return f"SKIP {meeting_id} — LLM parse empty"

    # ── Step 5: Salesforce ─────────────────────────────────────────────────
    sf_data           = query_sf_by_meeting_id(meeting_id)
    sf_interview_id   = sf_data.get("sf_interview_id", "")
    candidate_name    = sf_data.get("candidate_name") or parsed.get("candidate_name", "")
    date_of_interview = sf_data.get("date_of_interview", "")

    # ── Step 6: Extract path metadata ─────────────────────────────────────
    is_person = extract_is_person(base_prefix)
    year, month_num, month_name = extract_year_month(base_prefix)

    if not year:
        now_ist    = datetime.now(timezone.utc) + IST_OFFSET
        year       = str(now_ist.year)
        month_num  = now_ist.month
        month_name = now_ist.strftime("%B")

    log.info(f"[{meeting_id}] is_person='{is_person}' year={year} month={month_name}")

    # ── Step 7: Google Drive navigation ───────────────────────────────────
    drive_svc  = get_drive_service()
    sheets_svc = get_sheets_service()

    shared_drive_id = find_shared_drive_id(drive_svc)
    is_folder_id    = find_or_create_folder(drive_svc, GDRIVE_FOLDER, shared_drive_id, shared_drive_id)
    year_folder_id  = find_or_create_folder(drive_svc, year,          is_folder_id,    shared_drive_id)
    month_folder_id = find_or_create_folder(drive_svc, month_name,    year_folder_id,  shared_drive_id)

    sheet_name      = f"{month_name}_{year}"
    spreadsheet_id  = find_or_create_sheet(drive_svc, sheets_svc, sheet_name, month_folder_id, shared_drive_id)

    # ── Step 8: Build row data ─────────────────────────────────────────────
    date_str = date_of_interview or (datetime.now(timezone.utc) + IST_OFFSET).strftime("%Y-%m-%d")

    candidate_action = parsed.get("candidate_action_required", False)
    proxy_action     = parsed.get("proxy_support_action_required", False)
    chance_pct       = parsed.get("chance_of_moving_to_next_round", "")
    cac_str          = parsed.get("candidate_action_categories", "")
    pac_str          = parsed.get("proxy_support_action_categories", "")
    candidate_score  = parsed.get("candidate_score", "")
    proxy_score      = parsed.get("proxy_score", "")
    verdict          = parsed.get("verdict", "")
    round_type       = parsed.get("round_type", "")

    yn = lambda flag: "Yes" if flag else "No"

    # Routing logic
    # both false → write to ALL tabs anyway (Action Required = No)
    write_candidate = candidate_action or (not candidate_action and not proxy_action)
    write_is        = proxy_action     or (not candidate_action and not proxy_action)

    # ── Step 9: Write Data tab (ALWAYS) ───────────────────────────────────
    data_row = [
        date_str, sf_interview_id, candidate_name, is_person,
        meeting_id, str(chance_pct),
        yn(candidate_action), yn(proxy_action),
        cac_str, pac_str,
        candidate_score, proxy_score, verdict, round_type,
    ]
    append_row(sheets_svc, spreadsheet_id, "Data", data_row)
    log.info(f"[{meeting_id}] ✅ Data tab written")

    # ── Step 10: Candidate tab ─────────────────────────────────────────────
    if write_candidate:
        append_row(sheets_svc, spreadsheet_id, "Candidate", [
            date_str, sf_interview_id, candidate_name, meeting_id,
            str(chance_pct), yn(candidate_action), cac_str,
        ])
        log.info(f"[{meeting_id}] ✅ Candidate tab written")

    # ── Step 11: Interview-Success tab ────────────────────────────────────
    if write_is:
        append_row(sheets_svc, spreadsheet_id, "Interview-Success", [
            date_str, sf_interview_id, is_person, meeting_id,
            str(chance_pct), yn(proxy_action), pac_str,
        ])
        log.info(f"[{meeting_id}] ✅ Interview-Success tab written")

    # ── Step 12: Write sheets-done.json ───────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    write_s3_json(f"{temp_prefix}/sheets-done.json", {
        "meeting_id":       meeting_id,
        "status":           "sheets_written",
        "processed_at":     now_utc.isoformat(),
        "processed_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "spreadsheet_id":   spreadsheet_id,
        "sheet_name":       sheet_name,
        "llm_key":          llm_key,
        "base_prefix":      base_prefix,
        "tabs_written": {
            "Data":              True,
            "Candidate":         write_candidate,
            "Interview-Success": write_is,
        },
        "routing_reason": (
            "both_true"      if (candidate_action and proxy_action) else
            "candidate_only" if candidate_action else
            "proxy_only"     if proxy_action else
            "both_false"
        ),
        "sf_interview_id": sf_interview_id,
        "candidate_name":  candidate_name,
        "is_person":       is_person,
    })
    log.info(f"[{meeting_id}] ✅ sheets-done.json written")
    return f"OK {meeting_id} → {sheet_name} | spreadsheet={spreadsheet_id}"

# ── LIVE POLLER ───────────────────────────────────────────────────────────────

def live_poller():
    log.info("Live poller started")
    last_checked = datetime.now(timezone.utc) - timedelta(minutes=10)
    while True:
        try:
            items = scan_s3_for_unprocessed(since_modified=last_checked)
            last_checked = datetime.now(timezone.utc)
            new = 0
            for item in items:
                mid = item["meeting_id"]
                with _seen_lock:
                    if mid not in _seen:
                        _seen.add(mid)
                        try:
                            live_queue.put_nowait(item)
                            new += 1
                        except queue.Full:
                            log.warning(f"Live queue full — dropping {mid}")
            if new:
                log.info(f"Live poller: queued {new} new meetings")
        except Exception as e:
            log.error(f"Live poller error: {e}", exc_info=True)
        time.sleep(LIVE_POLL_INTERVAL)

# ── LIVE WORKERS ──────────────────────────────────────────────────────────────

def live_worker_loop():
    log.info("Live worker ready")
    while True:
        try:
            item = live_queue.get(timeout=60)
            try:
                result = process_one_meeting(item)
                log.info(f"[LIVE] {result}")
            except Exception as e:
                log.error(f"[LIVE] Error {item['meeting_id']}: {e}", exc_info=True)
            finally:
                live_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            log.error(f"Live worker error: {e}", exc_info=True)

# ── BACKFILL LOOP ─────────────────────────────────────────────────────────────

def backfill_loop():
    log.info("Backfill loop started")
    time.sleep(15)  # Let live workers start first
    while True:
        try:
            items = scan_s3_for_unprocessed()
            pending = []
            for item in items:
                mid = item["meeting_id"]
                with _seen_lock:
                    if mid not in _seen:
                        _seen.add(mid)
                        pending.append(item)

            if not pending:
                log.info(f"Backfill: nothing to process. Sleeping {BACKFILL_POLL_INTERVAL}s")
                time.sleep(BACKFILL_POLL_INTERVAL)
                continue

            log.info(f"Backfill: processing {len(pending)} meetings")
            with ThreadPoolExecutor(
                max_workers=BACKFILL_WORKERS,
                thread_name_prefix="backfill"
            ) as ex:
                futures = {ex.submit(process_one_meeting, item): item["meeting_id"] for item in pending}
                for future in as_completed(futures):
                    mid = futures[future]
                    try:
                        result = future.result()
                        log.info(f"[BACKFILL] {result}")
                    except Exception as e:
                        log.error(f"[BACKFILL] Error {mid}: {e}", exc_info=True)

            log.info(f"Backfill batch done. Sleeping {BACKFILL_POLL_INTERVAL}s")
            time.sleep(BACKFILL_POLL_INTERVAL)

        except Exception as e:
            log.error(f"Backfill loop error: {e}", exc_info=True)
            time.sleep(30)

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    log.info("sheets_writer_worker starting")

    try:
        get_google_creds()
        log.info("Google credentials ✅")
    except Exception as e:
        log.error(f"Google credentials failed: {e}")
        sys.exit(1)

    try:
        get_sf()
        log.info("Salesforce ✅")
    except Exception as e:
        log.error(f"Salesforce failed: {e}")
        sys.exit(1)

    try:
        drive_svc = get_drive_service()
        sid = find_shared_drive_id(drive_svc)
        log.info(f"Shared drive '{SHARED_DRIVE_NAME}': {sid} ✅")
    except Exception as e:
        log.error(f"Shared drive not found: {e}")
        sys.exit(1)

    # Start live poller
    threading.Thread(target=live_poller, daemon=True, name="live-poller").start()

    # Start backfill loop
    threading.Thread(target=backfill_loop, daemon=True, name="backfill-loop").start()

    # Start live worker threads
    live_threads = []
    for i in range(LIVE_WORKERS):
        t = threading.Thread(target=live_worker_loop, daemon=True, name=f"live-{i+1}")
        t.start()
        live_threads.append(t)

    log.info(f"All workers started: {LIVE_WORKERS} live + {BACKFILL_WORKERS} backfill")

    while True:
        time.sleep(60)
        alive = sum(1 for t in live_threads if t.is_alive())
        log.info(f"Heartbeat — live_workers={alive} queue={live_queue.qsize()}")

if __name__ == "__main__":
    main()
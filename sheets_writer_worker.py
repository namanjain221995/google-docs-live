"""
sheets_writer_worker.py
-----------------------
New service that runs AFTER llm_processor_worker.

Flow:
1. Scans S3 temp/ for meetings with llm-done.json but no sheets-done.json
2. Reads llm-done.json → gets llm.txt S3 path + meeting metadata
3. Reads llm.txt from S3 → parses JSON output
4. Queries Salesforce using Zoom Meeting ID → gets Interview ID (I-021337), date, candidate
5. Opens Google Sheets in 2026_Shared_Drive → Interview Success folder
6. Navigates/creates: Year folder → Month folder → one Sheet per month
7. Sheet has 3 tabs: Candidate | Interview-Success | Data
8. Routes rows based on candidate_action_required / proxy_support_action_required flags
9. Writes sheets-done.json to temp prefix to mark completion

Architecture:
- 20 workers total
- 10 "live" workers: poll an in-memory queue for newly completed LLM jobs
- 10 "backfill" workers: scan S3 for any llm-done.json without sheets-done.json
- Live queue is fed by a lightweight S3 event poller (checks every 30s for new llm-done.json)

Sheet Columns:
  Candidate sheet:      Date | Salesforce Interview ID | Candidate Name | Meeting ID |
                        Chance of Moving to Next Round % | Action Required | Candidate Action Categories

  Interview-Success:    Date | Salesforce Interview ID | Interview-Success Person | Meeting ID |
                        Chance of Moving to Next Round % | Action Required | Proxy Support Action Categories

  Data (all meetings):  Date | Salesforce Interview ID | Candidate Name | Interview-Success Person |
                        Meeting ID | Chance of Moving to Next Round % |
                        Candidate Action Required | Proxy Support Action Required |
                        Candidate Action Categories | Proxy Support Action Categories |
                        Candidate Score | Proxy Score | Verdict | Round Type

Routing:
  candidate_action_required=true, proxy=false  → Candidate sheet + Data
  proxy_support_action_required=true, cand=false → Interview-Success sheet + Data
  both true                                    → ALL 3 sheets
  both false                                   → ALL 3 sheets (Action Required = No)
  ALL meetings                                 → always Data sheet
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

# ── GLOBAL SHEETS RATE LIMITER ────────────────────────────────────────────────
# Google Sheets quota: 60 write requests/min/user.
# We cap at 45/min across ALL threads to stay safely under the limit.

_SHEETS_RATE_LIMIT  = 45     # max writes per window
_SHEETS_RATE_WINDOW = 60     # seconds
_sheets_write_lock  = threading.Lock()
_sheets_write_times = []     # timestamps of recent writes

def _acquire_sheets_token():
    """Block until a write slot is available within the rate window."""
    while True:
        with _sheets_write_lock:
            now = time.time()
            cutoff = now - _SHEETS_RATE_WINDOW
            global _sheets_write_times
            _sheets_write_times = [t for t in _sheets_write_times if t > cutoff]
            if len(_sheets_write_times) < _SHEETS_RATE_LIMIT:
                _sheets_write_times.append(now)
                return  # token acquired
            # wait until the oldest write leaves the window
            wait = _sheets_write_times[0] + _SHEETS_RATE_WINDOW - now + 0.1
        time.sleep(max(wait, 0.1))

# ── CONFIG ──────────────────────────────────────────────────────────────────

AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET         = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
SF_SECRET_NAME    = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
API_SECRET_NAME   = os.environ.get("API_SECRET_NAME", "secrets/api")   # has service-account key

SHARED_DRIVE_NAME = "2026_Shared_Drive"
GDRIVE_FOLDER     = "Interview Success"   # folder inside shared drive

TOTAL_WORKERS     = 20
LIVE_WORKERS      = 10     # consume from live_queue
BACKFILL_WORKERS  = 10     # scan S3 for missed meetings
LIVE_POLL_INTERVAL  = 30   # seconds — how often live poller checks S3
BACKFILL_POLL_INTERVAL = 120

IST_OFFSET = timedelta(hours=5, minutes=30)

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# ── LOGGING ─────────────────────────────────────────────────────────────────

os.makedirs("/home/ec2-user/google-docs-live/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ec2-user/google-docs-live/logs/sheets_writer_worker.log"),
    ]
)
log = logging.getLogger("sheets_writer_worker")

# ── AWS CLIENTS ──────────────────────────────────────────────────────────────

boto_config = Config(max_pool_connections=100)
s3 = boto3.client("s3", region_name=AWS_REGION, config=boto_config)
sm = boto3.client("secretsmanager", region_name=AWS_REGION)

# ── LIVE QUEUE (for newly finished LLM jobs) ─────────────────────────────────

live_queue = queue.Queue(maxsize=500)
_seen_llm_done = set()        # track what we've already queued (in-memory)
_seen_lock = threading.Lock()

# ── SECRET HELPERS ───────────────────────────────────────────────────────────

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

# ── GOOGLE AUTH ──────────────────────────────────────────────────────────────

_google_creds = None
_google_creds_lock = threading.Lock()

def get_google_creds():
    global _google_creds
    with _google_creds_lock:
        if _google_creds:
            return _google_creds
        secret = get_secret(API_SECRET_NAME)
        # secret has key "service-account" whose value is a JSON string or dict
        sa_raw = secret.get("service-account", secret)
        if isinstance(sa_raw, str):
            sa_info = json.loads(sa_raw)
        else:
            sa_info = sa_raw
        _google_creds = service_account.Credentials.from_service_account_info(
            sa_info, scopes=GOOGLE_SCOPES
        )
        return _google_creds

def get_drive_service():
    return build("drive", "v3", credentials=get_google_creds(), cache_discovery=False)

def get_sheets_service():
    return build("sheets", "v4", credentials=get_google_creds(), cache_discovery=False)

# ── SALESFORCE ────────────────────────────────────────────────────────────────

_sf_client = None
_sf_lock    = threading.Lock()

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
            domain="login"
        )
        return _sf_client

def query_salesforce_by_meeting_id(meeting_id: str) -> dict:
    """
    Returns dict with keys:
      sf_interview_id, candidate_name, company, date_of_interview, round_info,
      interviewer_name, recruiter_name
    Returns {} if not found.
    """
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
            log.warning(f"No SF record for meeting_id={meeting_id}")
            return {}
        r = records[0]
        return {
            "sf_interview_id":   r.get("Name", ""),          # e.g. I-021337
            "candidate_name":    r.get("Candidate_Name__c", ""),
            "company":           r.get("Company__c", ""),
            "date_of_interview": r.get("Date_of_Interview__c", ""),
            "round_info":        r.get("Round_Info__c", r.get("Round__c", "")),
            "interviewer_name":  r.get("Interviewer_s_Name__c", ""),
            "recruiter_name":    r.get("Recruiter_Name__c", ""),
        }
    except Exception as e:
        log.error(f"Salesforce query failed for meeting_id={meeting_id}: {e}")
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
        ContentType="application/json"
    )

def check_sheets_done(temp_prefix: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=f"{temp_prefix}/sheets-done.json")
        return True
    except Exception:
        return False

# ── LLM OUTPUT PARSER ─────────────────────────────────────────────────────────

def parse_llm_output(llm_txt: str) -> dict:
    """
    Parses llm.txt which contains a header block then JSON.
    Handles both pure JSON output and JSON embedded after the header.
    Returns a flat dict of the parsed fields we care about.
    """
    # Strip the header (everything before the first '{')
    json_start = llm_txt.find("{")
    if json_start == -1:
        log.warning("No JSON found in llm.txt — trying TOON fallback")
        return parse_toon_output(llm_txt)

    json_str = llm_txt[json_start:]
    # Find matching closing brace
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        # Try to extract just the first valid JSON object
        try:
            decoder = json.JSONDecoder()
            data, _ = decoder.raw_decode(json_str)
        except Exception as e:
            log.error(f"JSON parse failed: {e}")
            return {}

    # Flatten the fields we need
    summary_card = data.get("audit_summary_card", {})
    metadata     = data.get("audit_metadata", {})
    candidate_perf = data.get("overall_candidate_performance", {})
    proxy_perf   = data.get("overall_proxy_support_performance", {})
    verdict      = data.get("final_verdict", {})

    # candidate_action_categories → join as string
    cac = summary_card.get("candidate_action_categories", [])
    cac_str = "; ".join(
        item.get("category", "") for item in cac if isinstance(item, dict)
    ) if isinstance(cac, list) else str(cac)

    # proxy_support_action_categories → join
    pac = summary_card.get("proxy_support_action_categories", [])
    pac_str = "; ".join(
        item.get("category", "") for item in pac if isinstance(item, dict)
    ) if isinstance(pac, list) else str(pac)

    return {
        "candidate_name":                   summary_card.get("candidate_name", metadata.get("candidate_detected", "")),
        "chance_of_moving_to_next_round":   summary_card.get("chance_of_moving_to_next_round_percent", ""),
        "candidate_action_required":        summary_card.get("candidate_action_required", False),
        "proxy_support_action_required":    summary_card.get("proxy_support_action_required", False),
        "candidate_action_categories":      cac_str,
        "proxy_support_action_categories":  pac_str,
        "candidate_score":                  candidate_perf.get("score", ""),
        "proxy_score":                      proxy_perf.get("score", ""),
        "verdict":                          verdict.get("one_line_verdict", ""),
        "round_type":                       metadata.get("round_type_detected", ""),
    }

def parse_toon_output(toon_txt: str) -> dict:
    """
    Fallback TOON parser — extracts key fields from bracketed sections.
    Very basic — grabs what it can.
    """
    def extract_field(text, field_name):
        pattern = rf"{re.escape(field_name)}\s*[:=]\s*(.+)"
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(1).strip() if m else ""

    return {
        "candidate_name":                  extract_field(toon_txt, "candidate_name"),
        "chance_of_moving_to_next_round":  extract_field(toon_txt, "chance_of_moving_to_next_round_percent"),
        "candidate_action_required":       "true" in extract_field(toon_txt, "candidate_action_required").lower(),
        "proxy_support_action_required":   "true" in extract_field(toon_txt, "proxy_support_action_required").lower(),
        "candidate_action_categories":     extract_field(toon_txt, "candidate_action_categories"),
        "proxy_support_action_categories": extract_field(toon_txt, "proxy_support_action_categories"),
        "candidate_score":                 extract_field(toon_txt, "score"),
        "proxy_score":                     "",
        "verdict":                         extract_field(toon_txt, "one_line_verdict"),
        "round_type":                      extract_field(toon_txt, "round_type_detected"),
    }

# ── GOOGLE DRIVE HELPERS ──────────────────────────────────────────────────────

_drive_cache         = {}   # cache_key → id
_drive_cache_lock    = threading.Lock()
_folder_create_locks = {}   # cache_key → per-key Lock (prevents duplicate creates)
_folder_locks_lock   = threading.Lock()

def _get_folder_lock(cache_key: str) -> threading.Lock:
    """Return a per-folder-key lock so only one thread creates each folder."""
    with _folder_locks_lock:
        if cache_key not in _folder_create_locks:
            _folder_create_locks[cache_key] = threading.Lock()
        return _folder_create_locks[cache_key]

def find_shared_drive_id(drive_svc) -> str:
    """Find the ID of 2026_Shared_Drive. Cached after first lookup."""
    cache_key = "shared_drive_id"
    with _drive_cache_lock:
        if cache_key in _drive_cache:
            return _drive_cache[cache_key]

    folder_lock = _get_folder_lock(cache_key)
    with folder_lock:
        # Double-check after acquiring lock
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
    Find or create a folder by name inside parent_id.
    Uses per-key locking + double-check to prevent duplicate folder creation
    when multiple threads run simultaneously.
    """
    cache_key = f"{parent_id}/{name}"

    # Fast path: already cached — no lock needed
    with _drive_cache_lock:
        if cache_key in _drive_cache:
            return _drive_cache[cache_key]

    # Slow path: serialize per folder key
    folder_lock = _get_folder_lock(cache_key)
    with folder_lock:
        # Double-check: another thread may have created while we waited
        with _drive_cache_lock:
            if cache_key in _drive_cache:
                return _drive_cache[cache_key]

        # Search Google Drive for existing folder
        q = (
            f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
            f"and '{parent_id}' in parents and trashed=false"
        )
        resp = drive_svc.files().list(
            q=q,
            spaces="drive",
            fields="files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            corpora="drive",
            driveId=drive_id,
        ).execute()

        files = resp.get("files", [])
        if files:
            fid = files[0]["id"]
            if len(files) > 1:
                log.warning(f"Found {len(files)} duplicate folders named '{name}' — using first: {fid}")
        else:
            meta = {
                "name": name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            }
            f = drive_svc.files().create(
                body=meta,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            fid = f["id"]
            log.info(f"Created folder '{name}' under parent={parent_id} → {fid}")

        with _drive_cache_lock:
            _drive_cache[cache_key] = fid
        return fid

def find_or_create_sheet(drive_svc, sheets_svc, name: str, parent_id: str, drive_id: str) -> str:
    """
    Find or create a Google Sheet by name in parent_id.
    Uses per-key locking + double-check to prevent duplicate sheet creation.
    If created fresh, sets up the 3 tabs with correct headers.
    Returns the spreadsheet ID.
    """
    cache_key = f"sheet:{parent_id}/{name}"

    # Fast path
    with _drive_cache_lock:
        if cache_key in _drive_cache:
            return _drive_cache[cache_key]

    # Slow path: serialize per sheet key
    sheet_lock = _get_folder_lock(cache_key)
    with sheet_lock:
        # Double-check
        with _drive_cache_lock:
            if cache_key in _drive_cache:
                return _drive_cache[cache_key]

        q = (
            f"name='{name}' and mimeType='application/vnd.google-apps.spreadsheet' "
            f"and '{parent_id}' in parents and trashed=false"
        )
        resp = drive_svc.files().list(
            q=q,
            spaces="drive",
            fields="files(id, name)",
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
            meta = {
                "name": name,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [parent_id],
            }
            f = drive_svc.files().create(
                body=meta,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            sid = f["id"]
            log.info(f"Created new sheet '{name}': {sid}")
            setup_sheet_tabs(sheets_svc, sid)

        with _drive_cache_lock:
            _drive_cache[cache_key] = sid
        return sid

# ── SHEET TAB SETUP ───────────────────────────────────────────────────────────

CANDIDATE_HEADERS = [
    "Date", "Salesforce Interview ID", "Candidate Name", "Meeting ID",
    "Chance of Moving to Next Round %", "Action Required",
    "Candidate Action Categories"
]

INTERVIEW_SUCCESS_HEADERS = [
    "Date", "Salesforce Interview ID", "Interview-Success Person", "Meeting ID",
    "Chance of Moving to Next Round %", "Action Required",
    "Proxy Support Action Categories"
]

DATA_HEADERS = [
    "Date", "Salesforce Interview ID", "Candidate Name", "Interview-Success Person",
    "Meeting ID", "Chance of Moving to Next Round %",
    "Candidate Action Required", "Proxy Support Action Required",
    "Candidate Action Categories", "Proxy Support Action Categories",
    "Candidate Score", "Proxy Score", "Verdict", "Round Type"
]

TAB_NAMES = ["Candidate", "Interview-Success", "Data"]

def setup_sheet_tabs(sheets_svc, spreadsheet_id: str):
    """
    Rename Sheet1 → Candidate, add Interview-Success and Data tabs, write headers.
    """
    # Get current sheets
    meta = sheets_svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = meta.get("sheets", [])
    existing_titles = [s["properties"]["title"] for s in existing]

    requests = []

    # Rename first sheet to Candidate if needed
    if existing:
        first_id = existing[0]["properties"]["sheetId"]
        if existing[0]["properties"]["title"] != "Candidate":
            requests.append({
                "updateSheetProperties": {
                    "properties": {"sheetId": first_id, "title": "Candidate"},
                    "fields": "title"
                }
            })

    # Add missing tabs
    for tab in ["Interview-Success", "Data"]:
        if tab not in existing_titles:
            requests.append({
                "addSheet": {
                    "properties": {"title": tab}
                }
            })

    if requests:
        sheets_svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        log.info(f"Tabs set up for spreadsheet {spreadsheet_id}")

    # Write headers to each tab
    header_map = {
        "Candidate":         CANDIDATE_HEADERS,
        "Interview-Success": INTERVIEW_SUCCESS_HEADERS,
        "Data":              DATA_HEADERS,
    }
    data_ranges = []
    for tab_name, headers in header_map.items():
        data_ranges.append({
            "range": f"'{tab_name}'!A1",
            "values": [headers]
        })

    sheets_svc.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "RAW",
            "data": data_ranges
        }
    ).execute()
    log.info(f"Headers written to all 3 tabs for spreadsheet {spreadsheet_id}")

# ── APPEND ROW HELPER ─────────────────────────────────────────────────────────

def append_row(sheets_svc, spreadsheet_id: str, tab_name: str, row: list):
    """
    Append one row to the given tab.
    - Acquires a global rate-limiter token before each write (45 writes/min max)
    - Retries up to 5 times with exponential backoff on HTTP 429
    """
    max_retries = 5
    for attempt in range(max_retries):
        _acquire_sheets_token()   # wait for rate-limit slot
        try:
            sheets_svc.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab_name}'!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]}
            ).execute()
            return  # success
        except HttpError as e:
            if e.resp.status == 429:
                wait = (2 ** attempt) * 5   # 5s, 10s, 20s, 40s, 80s
                log.warning(f"429 on {tab_name} attempt {attempt+1}/{max_retries} — sleeping {wait}s")
                time.sleep(wait)
                if attempt == max_retries - 1:
                    log.error(f"append_row failed after {max_retries} retries for {tab_name}")
                    raise
            else:
                log.error(f"Failed to append row to {tab_name}: {e}")
                raise

# ── NAME HELPERS ──────────────────────────────────────────────────────────────

def clean_name(raw: str) -> str:
    """Replace underscores with spaces."""
    return raw.replace("_", " ").strip()

def extract_is_person_from_path(base_prefix: str) -> str:
    """
    base_prefix example:
    Interview-Success/Ronak_Thakar/2026/Month-5/Srikar_Tharala/...
    → returns 'Ronak Thakar'
    """
    parts = base_prefix.split("/")
    if len(parts) >= 2 and parts[0] == "Interview-Success":
        return clean_name(parts[1])
    return ""

def extract_year_month_from_path(base_prefix: str):
    """
    Returns (year_str, month_num, month_name) from the path.
    Interview-Success/Host/2026/Month-5/...
    """
    parts = base_prefix.split("/")
    year = ""
    month_folder = ""
    for p in parts:
        if p.isdigit() and len(p) == 4:
            year = p
        elif p.startswith("Month-"):
            month_folder = p
    month_num = int(month_folder.replace("Month-", "")) if month_folder else 0
    month_names = ["", "January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    month_name = month_names[month_num] if 0 < month_num <= 12 else month_folder
    return year, month_num, month_name

# ── CORE PROCESSOR ───────────────────────────────────────────────────────────

def process_one_meeting(item: dict) -> str:
    """
    item = {
        meeting_id, temp_prefix, llm_done_key, last_modified
    }
    """
    meeting_id  = item["meeting_id"]
    temp_prefix = item["temp_prefix"]

    log.info(f"[sheets] Processing meeting_id={meeting_id}")

    # ── Step 1: Read llm-done.json ──────────────────────────────────────────
    llm_done_raw = read_s3_text(f"{temp_prefix}/llm-done.json")
    if not llm_done_raw:
        return f"SKIP {meeting_id} — no llm-done.json content"
    try:
        llm_done = json.loads(llm_done_raw)
    except Exception:
        return f"SKIP {meeting_id} — llm-done.json parse error"

    llm_txt_path = llm_done.get("llm_txt", "")
    base_prefix  = llm_done.get("base_prefix", "")

    # llm_txt_path is like s3://bucket/key
    llm_s3_key = llm_txt_path.replace(f"s3://{S3_BUCKET}/", "") if llm_txt_path else ""

    # ── Step 2: Read llm.txt ────────────────────────────────────────────────
    llm_txt = ""
    if llm_s3_key:
        llm_txt = read_s3_text(llm_s3_key)
    if not llm_txt:
        # fallback: look next to done.json in temp
        llm_txt = read_s3_text(f"{temp_prefix}/llm.txt")
    if not llm_txt:
        return f"SKIP {meeting_id} — llm.txt not found"

    # ── Step 3: Parse LLM output ────────────────────────────────────────────
    parsed = parse_llm_output(llm_txt)
    if not parsed:
        return f"SKIP {meeting_id} — LLM parse returned empty"

    # ── Step 4: Salesforce lookup ────────────────────────────────────────────
    sf_data = query_salesforce_by_meeting_id(meeting_id)
    sf_interview_id  = sf_data.get("sf_interview_id", "")
    candidate_name   = sf_data.get("candidate_name") or parsed.get("candidate_name", "")
    date_of_interview = sf_data.get("date_of_interview", "")

    # ── Step 5: Extract path info ────────────────────────────────────────────
    is_person = extract_is_person_from_path(base_prefix)
    year, month_num, month_name = extract_year_month_from_path(base_prefix)

    if not year:
        now_ist = datetime.now(timezone.utc) + IST_OFFSET
        year = str(now_ist.year)
        month_num = now_ist.month
        month_name = now_ist.strftime("%B")

    # ── Step 6: Google Drive navigation ─────────────────────────────────────
    drive_svc  = get_drive_service()
    sheets_svc = get_sheets_service()

    # Find shared drive
    shared_drive_id = find_shared_drive_id(drive_svc)

    # Find 'Interview Success' root folder inside shared drive
    is_folder_id = find_or_create_folder(drive_svc, GDRIVE_FOLDER, shared_drive_id, shared_drive_id)

    # Year folder
    year_folder_id = find_or_create_folder(drive_svc, year, is_folder_id, shared_drive_id)

    # Month folder
    month_folder_id = find_or_create_folder(drive_svc, month_name, year_folder_id, shared_drive_id)

    # Sheet name = "Month_Year" e.g. "May_2026"
    sheet_name = f"{month_name}_{year}"
    spreadsheet_id = find_or_create_sheet(drive_svc, sheets_svc, sheet_name, month_folder_id, shared_drive_id)

    # ── Step 7: Build row data ───────────────────────────────────────────────
    date_str = date_of_interview or (datetime.now(timezone.utc) + IST_OFFSET).strftime("%Y-%m-%d")

    candidate_action    = parsed.get("candidate_action_required", False)
    proxy_action        = parsed.get("proxy_support_action_required", False)
    chance_pct          = parsed.get("chance_of_moving_to_next_round", "")
    cac_str             = parsed.get("candidate_action_categories", "")
    pac_str             = parsed.get("proxy_support_action_categories", "")
    candidate_score     = parsed.get("candidate_score", "")
    proxy_score         = parsed.get("proxy_score", "")
    verdict             = parsed.get("verdict", "")
    round_type          = parsed.get("round_type", "")

    action_required_yes_no = lambda flag: "Yes" if flag else "No"

    # ── Step 8: Write to Data tab (ALWAYS) ──────────────────────────────────
    data_row = [
        date_str,
        sf_interview_id,
        candidate_name,
        is_person,
        meeting_id,
        str(chance_pct),
        action_required_yes_no(candidate_action),
        action_required_yes_no(proxy_action),
        cac_str,
        pac_str,
        str(candidate_score),
        str(proxy_score),
        verdict,
        round_type,
    ]
    append_row(sheets_svc, spreadsheet_id, "Data", data_row)
    log.info(f"[{meeting_id}] Written to Data tab")

    # ── Step 9: Routing logic ────────────────────────────────────────────────
    #
    # candidate_action=true  → Candidate tab
    # proxy_action=true      → Interview-Success tab
    # both true              → BOTH tabs
    # both false             → BOTH tabs (still write everywhere, Action Required = No)
    # ALL meetings           → Data tab always
    #
    write_candidate_tab     = candidate_action or (not candidate_action and not proxy_action)
    write_is_tab            = proxy_action     or (not candidate_action and not proxy_action)

    # ── Step 9: Write to Candidate tab ───────────────────────────────────────
    if write_candidate_tab:
        candidate_row = [
            date_str,
            sf_interview_id,
            candidate_name,
            meeting_id,
            str(chance_pct),
            action_required_yes_no(candidate_action),   # "No" when both false
            cac_str,
        ]
        append_row(sheets_svc, spreadsheet_id, "Candidate", candidate_row)
        log.info(f"[{meeting_id}] Written to Candidate tab (action={candidate_action})")

    # ── Step 10: Write to Interview-Success tab ───────────────────────────────
    if write_is_tab:
        is_row = [
            date_str,
            sf_interview_id,
            is_person,
            meeting_id,
            str(chance_pct),
            action_required_yes_no(proxy_action),       # "No" when both false
            pac_str,
        ]
        append_row(sheets_svc, spreadsheet_id, "Interview-Success", is_row)
        log.info(f"[{meeting_id}] Written to Interview-Success tab (action={proxy_action})")

    # ── Step 11: Write sheets-done.json to temp ──────────────────────────────
    now_utc = datetime.now(timezone.utc)
    done_data = {
        "meeting_id":       meeting_id,
        "status":           "sheets_written",
        "processed_at":     now_utc.isoformat(),
        "processed_at_ist": (now_utc + IST_OFFSET).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "spreadsheet_id":   spreadsheet_id,
        "sheet_name":       sheet_name,
        "tabs_written": {
            "Data":              True,
            "Candidate":         write_candidate_tab,
            "Interview-Success": write_is_tab,
        },
        "routing_reason": (
            "both_true"   if (candidate_action and proxy_action) else
            "both_false"  if (not candidate_action and not proxy_action) else
            "candidate_only" if candidate_action else
            "proxy_only"
        ),
        "sf_interview_id":  sf_interview_id,
        "candidate_name":   candidate_name,
        "is_person":        is_person,
    }
    write_s3_json(f"{temp_prefix}/sheets-done.json", done_data)
    log.info(f"[{meeting_id}] sheets-done.json written ✅")

    return f"OK {meeting_id} → spreadsheet={spreadsheet_id} sheet={sheet_name}"

# ── S3 SCANNER ───────────────────────────────────────────────────────────────

def scan_s3_for_unprocessed(since_modified=None) -> list:
    """
    Scan temp/live-doc-history/ for meetings with llm-done.json but no sheets-done.json.
    If since_modified is set, only return meetings modified after that timestamp.
    Returns list of item dicts.
    """
    paginator = s3.get_paginator("list_objects_v2")

    has_llm_done   = {}
    has_sheets_done = set()

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key           = obj["Key"]
            last_modified = obj.get("LastModified")
            parts         = key.split("/")

            # NEW structure: temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/meeting_id/file
            if (len(parts) == 7
                    and parts[2].isdigit()
                    and parts[3].startswith("Month-")):
                meeting_id = parts[5]
                filename   = parts[6]
                prefix     = "/".join(parts[:6])
            # OLD structure: temp/live-doc-history/meeting_id/file
            elif len(parts) == 4 and parts[2].isdigit():
                meeting_id = parts[2]
                filename   = parts[3]
                prefix     = "/".join(parts[:3])
            else:
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

    unprocessed = []
    for mid, info in has_llm_done.items():
        if mid not in has_sheets_done:
            unprocessed.append({
                "meeting_id":  mid,
                "temp_prefix": info["prefix"],
                "llm_done_key": info["key"],
                "last_modified": info["last_modified"],
            })

    # Newest first
    unprocessed.sort(
        key=lambda x: x["last_modified"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )
    return unprocessed

# ── LIVE POLLER ──────────────────────────────────────────────────────────────

def live_poller():
    """
    Runs in background. Every LIVE_POLL_INTERVAL seconds, scans S3 for
    NEW llm-done.json files and pushes them to live_queue.
    """
    log.info("Live poller started")
    # Start from 'now minus 10 minutes' to catch very recent ones
    last_checked = datetime.now(timezone.utc) - timedelta(minutes=10)

    while True:
        try:
            items = scan_s3_for_unprocessed(since_modified=last_checked)
            last_checked = datetime.now(timezone.utc)

            new_count = 0
            for item in items:
                mid = item["meeting_id"]
                with _seen_lock:
                    if mid not in _seen_llm_done:
                        _seen_llm_done.add(mid)
                        try:
                            live_queue.put_nowait(item)
                            new_count += 1
                        except queue.Full:
                            log.warning(f"Live queue full — dropping {mid}")

            if new_count:
                log.info(f"Live poller: queued {new_count} new meetings")

        except Exception as e:
            log.error(f"Live poller error: {e}", exc_info=True)

        time.sleep(LIVE_POLL_INTERVAL)

# ── LIVE WORKER ──────────────────────────────────────────────────────────────

def live_worker_loop():
    """Consumes from live_queue. Runs in 10 threads."""
    log.info("Live worker started")
    while True:
        try:
            item = live_queue.get(timeout=60)
            try:
                result = process_one_meeting(item)
                log.info(f"[LIVE] {result}")
            except Exception as e:
                log.error(f"[LIVE] Error for {item['meeting_id']}: {e}", exc_info=True)
            finally:
                live_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            log.error(f"Live worker outer error: {e}", exc_info=True)

# ── BACKFILL LOOP ────────────────────────────────────────────────────────────

def backfill_loop():
    """
    Runs continuously. Every BACKFILL_POLL_INTERVAL seconds, finds ALL
    llm-done-but-no-sheets-done meetings and processes them with 10 workers.
    """
    log.info("Backfill loop started")
    # Give live workers a head start on startup
    time.sleep(30)

    while True:
        try:
            items = scan_s3_for_unprocessed()
            # Skip any already in live_queue or seen
            pending = []
            for item in items:
                mid = item["meeting_id"]
                with _seen_lock:
                    if mid not in _seen_llm_done:
                        _seen_llm_done.add(mid)
                        pending.append(item)

            if not pending:
                log.info(f"Backfill: nothing to process. Sleeping {BACKFILL_POLL_INTERVAL}s")
                time.sleep(BACKFILL_POLL_INTERVAL)
                continue

            log.info(f"Backfill: processing {len(pending)} meetings with {BACKFILL_WORKERS} workers")
            with ThreadPoolExecutor(max_workers=BACKFILL_WORKERS, thread_name_prefix="backfill") as ex:
                futures = {ex.submit(process_one_meeting, item): item["meeting_id"] for item in pending}
                for future in as_completed(futures):
                    mid = futures[future]
                    try:
                        result = future.result()
                        log.info(f"[BACKFILL] {result}")
                    except Exception as e:
                        log.error(f"[BACKFILL] Error for {mid}: {e}", exc_info=True)

            log.info(f"Backfill batch complete. Sleeping {BACKFILL_POLL_INTERVAL}s")
            time.sleep(BACKFILL_POLL_INTERVAL)

        except Exception as e:
            log.error(f"Backfill loop error: {e}", exc_info=True)
            time.sleep(30)

# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info(f"sheets_writer_worker starting — live_workers={LIVE_WORKERS} backfill_workers={BACKFILL_WORKERS}")

    # Verify Google credentials
    try:
        get_google_creds()
        log.info("Google credentials loaded ✅")
    except Exception as e:
        log.error(f"Google credentials failed: {e}")
        sys.exit(1)

    # Verify Salesforce
    try:
        get_sf()
        log.info("Salesforce connection established ✅")
    except Exception as e:
        log.error(f"Salesforce connection failed: {e}")
        sys.exit(1)

    # Verify shared drive accessible
    try:
        drive_svc = get_drive_service()
        sid = find_shared_drive_id(drive_svc)
        log.info(f"Shared drive '{SHARED_DRIVE_NAME}' found: {sid} ✅")
    except Exception as e:
        log.error(f"Shared drive not found: {e}")
        sys.exit(1)

    # Start live poller (background thread)
    poller_thread = threading.Thread(target=live_poller, daemon=True, name="live-poller")
    poller_thread.start()

    # Start backfill loop (background thread)
    backfill_thread = threading.Thread(target=backfill_loop, daemon=True, name="backfill-loop")
    backfill_thread.start()

    # Start 10 live worker threads
    live_threads = []
    for i in range(LIVE_WORKERS):
        t = threading.Thread(target=live_worker_loop, daemon=True, name=f"live-worker-{i+1}")
        t.start()
        live_threads.append(t)

    log.info(f"All workers started. {LIVE_WORKERS} live + {BACKFILL_WORKERS} backfill via ThreadPoolExecutor")

    # Keep main thread alive
    while True:
        time.sleep(60)
        alive_live = sum(1 for t in live_threads if t.is_alive())
        log.info(f"Heartbeat — live_workers_alive={alive_live} live_queue_size={live_queue.qsize()}")

if __name__ == "__main__":
    main()
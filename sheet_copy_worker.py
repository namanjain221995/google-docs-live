"""
sheet_copy_worker.py
====================
LIVE MIRROR SERVICE — master month-sheet → "Interview Success copy"

WHAT IT DOES
------------
The master spreadsheet for each month lives at:

    2026_Shared_Drive / Interview Success / <year> / <month> / <Month>_<Year>

…and contains the tabs "Candidate" and "Interview-Success" (plus "Data").

This service continuously mirrors those two tabs into SEPARATE spreadsheets
under a parallel folder tree:

    2026_Shared_Drive / Interview Success copy / <year> / <month> / Candidate
    2026_Shared_Drive / Interview Success copy / <year> / <month> / Interview-Success

The master is edited live (existing cells change, rows are added/removed), so
the copy is a TRUE MIRROR: every sync makes the destination exactly equal the
source — new rows, edited rows, and deletions are all reflected.

WORKER MODEL
------------
10 workers total: 5 dedicated to "Candidate" jobs, 5 to "Interview-Success"
jobs (two queues). Discovery enqueues one job per (month, tab) each cycle.

INTEGRITY (this is the important part)
--------------------------------------
1. DATA PATH IS DETERMINISTIC. Cells are copied verbatim (RAW). No LLM ever
   writes data, so values can never be altered/hallucinated.
2. DETERMINISTIC VERIFY (authoritative). After each write the copy is read back
   and compared cell-by-cell to the source. A mismatch is logged loudly.
3. OPENAI (gpt-5-nano) IS ADVISORY ONLY. On changed data it validates a sample
   of rows against the known schema and flags anomalies (bad date / id / enum).
   It is fully wrapped — an OpenAI outage can never break or block the copy.

COST SAFETY
-----------
Each cycle first checks the master's Drive modifiedTime + a content hash. If
nothing changed since the last successful sync, the job does ZERO Sheets writes
and ZERO OpenAI calls. Idle cycles are nearly free.

CLI
---
    python3.11 sheet_copy_worker.py                 # run forever (service mode)
    python3.11 sheet_copy_worker.py --once          # one cycle, then exit
    python3.11 sheet_copy_worker.py --dry-run       # discover + log, no writes
    python3.11 sheet_copy_worker.py --no-openai     # skip OpenAI validation
    python3.11 sheet_copy_worker.py --force         # ignore change-detection
    python3.11 sheet_copy_worker.py --year 2026 --month May   # restrict to one month
"""

import os
import re
import sys
import json
import time
import hashlib
import logging
import argparse
import threading
import urllib.request
import urllib.error
from queue import Queue
from datetime import datetime, timezone, timedelta

import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── CONFIG ────────────────────────────────────────────────────────────────────
AWS_REGION         = os.environ.get("AWS_REGION", "us-east-1")
API_SECRET_NAME    = os.environ.get("API_SECRET_NAME", "secrets/api")

SHARED_DRIVE_NAME  = os.environ.get("SHARED_DRIVE_NAME", "2026_Shared_Drive")
SOURCE_ROOT_FOLDER = os.environ.get("COPY_SOURCE_ROOT", "Interview Success")
DEST_ROOT_FOLDER   = os.environ.get("COPY_DEST_ROOT",   "Interview Success copy")

# Tabs we mirror, and the destination spreadsheet name we mirror each into.
# (Each becomes its OWN spreadsheet inside the month folder — never combined.)
TAB_TO_DEST_SHEET = {
    "Candidate":         os.environ.get("COPY_DEST_CANDIDATE_NAME",  "Candidate"),
    "Interview-Success": os.environ.get("COPY_DEST_IS_NAME",         "Interview-Success"),
}

CANDIDATE_WORKERS = int(os.environ.get("COPY_CANDIDATE_WORKERS", "5"))
IS_WORKERS        = int(os.environ.get("COPY_IS_WORKERS",        "5"))
POLL_INTERVAL     = int(os.environ.get("COPY_POLL_INTERVAL",     "120"))  # seconds

APP_HOME   = os.environ.get("APP_HOME", "/home/ec2-user/google-docs-live")
LOG_DIR    = os.path.join(APP_HOME, "logs")
STATE_DIR  = os.path.join(APP_HOME, "state")
STATE_FILE = os.path.join(STATE_DIR, "sheet_copy_state.json")

# OpenAI (advisory validation only)
OPENAI_API_KEY     = os.environ.get("OPENAI_API_KEY", "")          # env first…
OPENAI_MODEL       = os.environ.get("OPENAI_MODEL", "gpt-5-nano")
OPENAI_ENABLED     = os.environ.get("COPY_OPENAI_ENABLED", "true").lower() == "true"
OPENAI_SAMPLE_ROWS = int(os.environ.get("COPY_OPENAI_SAMPLE_ROWS", "40"))
OPENAI_TIMEOUT     = int(os.environ.get("COPY_OPENAI_TIMEOUT", "60"))

IST = timedelta(hours=5, minutes=30)

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

_MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"]
_MONTH_SET   = set(_MONTH_NAMES)

# Known schema per tab — used by the deterministic + OpenAI validators.
TAB_SCHEMA = {
    "Candidate": [
        "Date", "Salesforce Interview ID", "Candidate Name", "Meeting ID",
        "Chance of Moving to Next Round %", "Action Required",
        "Candidate Action Categories",
    ],
    "Interview-Success": [
        "Date", "Salesforce Interview ID", "Interview-Success Person", "Meeting ID",
        "Chance of Moving to Next Round %", "Action Required",
        "Interview-Success Action Categories",
    ],
}
_ACTION_REQUIRED_ENUM = {"CRITICAL", "NEEDS_IMPROVEMENT", "GOOD", "EXCELLENT"}

# ── LOGGING ───────────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "sheet_copy_worker.log"),
                            encoding="utf-8"),
    ],
)
log = logging.getLogger("sheet_copy")


def now_ist_str() -> str:
    return (datetime.now(timezone.utc) + IST).strftime("%Y-%m-%d %I:%M:%S %p IST")


# ── RATE LIMITER: shared across all workers (Sheets per-user quota ≈ 60/min) ────
_RL_LIMIT = int(os.environ.get("COPY_RL_LIMIT", "50"))
_RL_WIN   = 60
_rl_lock  = threading.Lock()
_rl_times = []


def _token():
    """Block until a Sheets-API token is available (token bucket)."""
    global _rl_times
    while True:
        with _rl_lock:
            now = time.time()
            _rl_times = [t for t in _rl_times if t > now - _RL_WIN]
            if len(_rl_times) < _RL_LIMIT:
                _rl_times.append(now)
                return
            wait = _rl_times[0] + _RL_WIN - now + 0.1
        time.sleep(max(wait, 0.1))


# ── AWS SECRETS ───────────────────────────────────────────────────────────────
_smc       = boto3.client("secretsmanager", region_name=AWS_REGION)
_sec_cache = {}
_sec_lock  = threading.Lock()


def get_secret(name):
    with _sec_lock:
        if name not in _sec_cache:
            _sec_cache[name] = json.loads(
                _smc.get_secret_value(SecretId=name)["SecretString"]
            )
        return _sec_cache[name]


# ── GOOGLE CREDS / SERVICES ───────────────────────────────────────────────────
_gcreds    = None
_gcreds_lk = threading.Lock()


def gcreds():
    global _gcreds
    with _gcreds_lk:
        if _gcreds:
            return _gcreds
        sec    = get_secret(API_SECRET_NAME)
        sa_raw = sec.get("service-account", sec)
        sa     = json.loads(sa_raw) if isinstance(sa_raw, str) else sa_raw
        _gcreds = service_account.Credentials.from_service_account_info(
            sa, scopes=GOOGLE_SCOPES
        )
        return _gcreds


def drive_svc():
    return build("drive", "v3", credentials=gcreds(), cache_discovery=False)


def sheets_svc():
    return build("sheets", "v4", credentials=gcreds(), cache_discovery=False)


# ── OPENAI KEY (env first, then secrets/api) ──────────────────────────────────
_openai_key = OPENAI_API_KEY
_openai_lk  = threading.Lock()


def get_openai_key() -> str:
    global _openai_key
    with _openai_lk:
        if _openai_key:
            return _openai_key
        try:
            _openai_key = get_secret(API_SECRET_NAME).get("OPENAI_API_KEY", "")
        except Exception as e:
            log.warning(f"Could not load OPENAI_API_KEY from {API_SECRET_NAME}: {e}")
            _openai_key = ""
        return _openai_key


def _is_reasoning_model(model: str) -> bool:
    m = model.lower()
    return m.startswith("gpt-5") or m.startswith("o1") or m.startswith("o3") or m.startswith("o4")


def call_openai(messages: list, max_out: int = 3000) -> str:
    """
    Minimal, dependency-free OpenAI chat call (mirrors llm_processor_worker).
    Adapts params for reasoning models (gpt-5-*/o*). Returns "" on any failure
    — OpenAI here is advisory only and must never raise into the copy path.
    """
    api_key = get_openai_key()
    if not api_key:
        return ""

    payload = {"model": OPENAI_MODEL, "messages": messages,
               "response_format": {"type": "json_object"}}
    if _is_reasoning_model(OPENAI_MODEL):
        payload["max_completion_tokens"] = max_out
        payload["reasoning_effort"] = "low"
    else:
        payload["max_tokens"] = max_out
        payload["temperature"] = 0

    def _post(body: dict) -> str:
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=OPENAI_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data["choices"][0]["message"]["content"]

    try:
        return _post(payload)
    except urllib.error.HTTPError as e:
        # Retry once without optional params some models reject (response_format / temperature).
        try:
            detail = e.read().decode("utf-8", "ignore")
        except Exception:
            detail = str(e)
        log.warning(f"OpenAI HTTP {e.code}: {detail[:300]} — retrying minimal payload")
        minimal = {"model": OPENAI_MODEL, "messages": messages}
        minimal["max_completion_tokens" if _is_reasoning_model(OPENAI_MODEL)
                else "max_tokens"] = max_out
        try:
            return _post(minimal)
        except Exception as e2:
            log.warning(f"OpenAI retry failed: {e2}")
            return ""
    except Exception as e:
        log.warning(f"OpenAI call failed: {e}")
        return ""


# ── DRIVE / SHEETS CACHE + RACE-SAFE FOLDER CREATE ────────────────────────────
_dc    = {}                 # id cache: folder/sheet ids by key
_dc_lk = threading.Lock()
_cl    = {}                 # per-key locks
_cl_lk = threading.Lock()


def _lk(key):
    with _cl_lk:
        if key not in _cl:
            _cl[key] = threading.Lock()
        return _cl[key]


def shared_drive_id(dsvc) -> str:
    k = "__sd__"
    with _dc_lk:
        if k in _dc:
            return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc:
                return _dc[k]
        for d in dsvc.drives().list(pageSize=50).execute().get("drives", []):
            if d["name"] == SHARED_DRIVE_NAME:
                with _dc_lk:
                    _dc[k] = d["id"]
                return d["id"]
        raise ValueError(f"Shared drive '{SHARED_DRIVE_NAME}' not found")


def _escape(name: str) -> str:
    return name.replace("\\", "\\\\").replace("'", "\\'")


def find_folder(dsvc, name, parent, drive_id):
    """Find a folder by name under parent. Returns id or None (no create)."""
    resp = dsvc.files().list(
        q=(f"name='{_escape(name)}' and "
           f"mimeType='application/vnd.google-apps.folder' "
           f"and '{parent}' in parents and trashed=false"),
        spaces="drive", fields="files(id,name)",
        includeItemsFromAllDrives=True, supportsAllDrives=True,
        corpora="drive", driveId=drive_id,
    ).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def ensure_folder(dsvc, name, parent, drive_id):
    """Find-or-create a folder (cached, race-safe). Used for DESTINATION tree."""
    k = f"f:{parent}:{name}"
    with _dc_lk:
        if k in _dc:
            return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc:
                return _dc[k]
        fid = find_folder(dsvc, name, parent, drive_id)
        if fid is None:
            fid = dsvc.files().create(
                body={"name": name,
                      "mimeType": "application/vnd.google-apps.folder",
                      "parents": [parent]},
                fields="id", supportsAllDrives=True,
            ).execute()["id"]
            log.info(f"Created folder '{name}' → {fid}")
        with _dc_lk:
            _dc[k] = fid
        return fid


def list_subfolders(dsvc, parent, drive_id):
    """List immediate child folders of parent. Returns [(name, id), ...]."""
    out, page = [], None
    while True:
        resp = dsvc.files().list(
            q=(f"mimeType='application/vnd.google-apps.folder' "
               f"and '{parent}' in parents and trashed=false"),
            spaces="drive", fields="nextPageToken, files(id,name)",
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            corpora="drive", driveId=drive_id, pageSize=200, pageToken=page,
        ).execute()
        out.extend((f["name"], f["id"]) for f in resp.get("files", []))
        page = resp.get("nextPageToken")
        if not page:
            break
    return out


def find_spreadsheet(dsvc, name, parent, drive_id):
    """Find a spreadsheet by exact name under parent. Returns id or None."""
    resp = dsvc.files().list(
        q=(f"name='{_escape(name)}' and "
           f"mimeType='application/vnd.google-apps.spreadsheet' "
           f"and '{parent}' in parents and trashed=false"),
        spaces="drive", fields="files(id,name,modifiedTime)",
        includeItemsFromAllDrives=True, supportsAllDrives=True,
        corpora="drive", driveId=drive_id,
    ).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def file_modified_time(dsvc, file_id) -> str:
    """Drive modifiedTime (cheap, separate quota from Sheets). '' on error."""
    try:
        return dsvc.files().get(
            fileId=file_id, fields="modifiedTime", supportsAllDrives=True,
        ).execute().get("modifiedTime", "")
    except Exception as e:
        log.warning(f"modifiedTime get failed for {file_id}: {e}")
        return ""


def ensure_dest_spreadsheet(dsvc, ssvc, name, tab_name, parent, drive_id):
    """
    Find-or-create the destination spreadsheet (one tab named `tab_name`).
    Cached + race-safe. Returns the spreadsheet id.
    """
    k = f"s:{parent}:{name}"
    with _dc_lk:
        if k in _dc:
            return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc:
                return _dc[k]
        sid = find_spreadsheet(dsvc, name, parent, drive_id)
        if sid is None:
            sid = dsvc.files().create(
                body={"name": name,
                      "mimeType": "application/vnd.google-apps.spreadsheet",
                      "parents": [parent]},
                fields="id", supportsAllDrives=True,
            ).execute()["id"]
            log.info(f"Created spreadsheet '{name}' → {sid}")
        _ensure_single_tab(ssvc, sid, tab_name)
        with _dc_lk:
            _dc[k] = sid
        return sid


def _ensure_single_tab(ssvc, sid, tab_name):
    """Guarantee a tab named `tab_name` exists (rename default Sheet1, else add)."""
    _token()
    meta   = ssvc.spreadsheets().get(spreadsheetId=sid).execute()
    sheets = meta.get("sheets", [])
    titles = [s["properties"]["title"] for s in sheets]
    if tab_name in titles:
        return
    if len(sheets) == 1:                       # rename the lone default tab
        req = {"updateSheetProperties": {
            "properties": {"sheetId": sheets[0]["properties"]["sheetId"],
                           "title": tab_name},
            "fields": "title"}}
    else:
        req = {"addSheet": {"properties": {"title": tab_name}}}
    _token()
    ssvc.spreadsheets().batchUpdate(
        spreadsheetId=sid, body={"requests": [req]}
    ).execute()
    log.info(f"Ensured tab '{tab_name}' on {sid}")


# ── SHEETS READ / WRITE / CLEAR (with 429 backoff) ────────────────────────────
def _a1(tab: str) -> str:
    return "'" + tab.replace("'", "''") + "'"


def _retry(fn, what: str):
    for attempt in range(6):
        _token()
        try:
            return fn()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status == 429:
                wait = min((2 ** attempt) * 3, 30)
                log.warning(f"429 on {what} (attempt {attempt+1}/6) — sleep {wait}s")
                time.sleep(wait)
                if attempt == 5:
                    raise
            else:
                raise


def read_tab(ssvc, sid, tab):
    """Read the full used range of a tab as displayed text (FORMATTED_VALUE)."""
    def _do():
        return ssvc.spreadsheets().values().get(
            spreadsheetId=sid, range=_a1(tab),
            valueRenderOption="FORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
    resp = _retry(_do, f"read {tab}")
    return resp.get("values", [])


def write_tab(ssvc, sid, tab, values):
    """RAW write starting at A1 (verbatim text mirror — never reinterpreted)."""
    def _do():
        return ssvc.spreadsheets().values().update(
            spreadsheetId=sid, range=f"{_a1(tab)}!A1",
            valueInputOption="RAW", body={"values": values},
        ).execute()
    _retry(_do, f"write {tab}")


def clear_below(ssvc, sid, tab, keep_rows):
    """Clear trailing rows beyond `keep_rows` (reflects source deletions).
    Tolerates the case where there is nothing past the data to clear (the
    clear range can start beyond the grid right after a first write)."""
    rng = _a1(tab) if keep_rows <= 0 else f"{_a1(tab)}!A{keep_rows + 1}:Z"
    def _do():
        return ssvc.spreadsheets().values().clear(
            spreadsheetId=sid, range=rng, body={}
        ).execute()
    try:
        _retry(_do, f"clear {tab}")
    except HttpError as e:
        msg = str(e)
        if getattr(e.resp, "status", None) == 400 and \
                ("Unable to parse range" in msg or "exceeds grid limits" in msg):
            return  # nothing beyond the data to clear — fine
        raise


# ── VALIDATION ────────────────────────────────────────────────────────────────
_RE_DATE    = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RE_SFID    = re.compile(r"^I-\d+$")
_RE_MEETING = re.compile(r"^\d+$")


def deterministic_schema_warnings(tab, values):
    """Cheap local sanity checks on the SOURCE data. Advisory only (master is
    the source of truth — we still mirror it exactly). Returns list of strings."""
    warnings = []
    if not values:
        return warnings
    expected = TAB_SCHEMA.get(tab, [])
    header   = [c.strip() for c in values[0]]
    if expected and header[:len(expected)] != expected:
        warnings.append(f"header mismatch: got {header} expected {expected}")
    for r, row in enumerate(values[1:], start=2):
        cell = lambda i: (row[i].strip() if i < len(row) and row[i] is not None else "")
        if cell(0) and not _RE_DATE.match(cell(0)):
            warnings.append(f"row {r}: bad Date '{cell(0)}'")
        sf = cell(1)
        if sf and sf != "NOT_FOUND" and not _RE_SFID.match(sf):
            warnings.append(f"row {r}: bad SF ID '{sf}'")
        if cell(3) and not _RE_MEETING.match(cell(3)):
            warnings.append(f"row {r}: bad Meeting ID '{cell(3)}'")
        ch = cell(4)
        if ch and not (ch.isdigit() and 0 <= int(ch) <= 100):
            warnings.append(f"row {r}: bad Chance '{ch}'")
        ar = cell(5)
        if ar and ar not in _ACTION_REQUIRED_ENUM:
            warnings.append(f"row {r}: bad Action Required '{ar}'")
    return warnings


def openai_validate(tab, values, tag):
    """
    ADVISORY ONLY. Ask gpt-5-nano to flag schema/anomaly issues in a row sample.
    Never raises; logs flagged issues as warnings. Does not touch any data.
    """
    if not OPENAI_ENABLED or len(values) < 2:
        return
    header  = values[0]
    sample  = values[1:1 + OPENAI_SAMPLE_ROWS]
    schema  = TAB_SCHEMA.get(tab, header)
    sys_msg = (
        "You are a data-quality validator for an interview-tracking spreadsheet. "
        "You ONLY inspect data and report problems. You never rewrite data. "
        "Respond with strict JSON: "
        '{"ok": boolean, "issues": ["short human-readable issue strings"]}.'
    )
    user_msg = (
        f"Tab: {tab}\n"
        f"Expected columns (in order): {schema}\n"
        "Rules: Date is YYYY-MM-DD; Salesforce Interview ID is I-NNNNNN or NOT_FOUND; "
        "Meeting ID is digits only; Chance is an integer 0-100; "
        "Action Required is one of CRITICAL|NEEDS_IMPROVEMENT|GOOD|EXCELLENT; "
        "the categories column is free comma-separated text. "
        "Flag rows that clearly violate these rules or look like a header/garbage "
        "row mixed into the data. Report row numbers (1 = first data row below).\n\n"
        f"Header row: {header}\n"
        f"Data rows (JSON): {json.dumps(sample, ensure_ascii=False)}"
    )
    raw = call_openai([{"role": "system", "content": sys_msg},
                       {"role": "user", "content": user_msg}])
    if not raw:
        return
    try:
        verdict = json.loads(raw)
    except Exception:
        log.info(f"{tag} OpenAI validation (unparsed): {raw[:200]}")
        return
    issues = verdict.get("issues") or []
    if verdict.get("ok") and not issues:
        log.info(f"{tag} OpenAI validation: OK ({len(sample)} rows sampled)")
    else:
        for i in issues[:25]:
            log.warning(f"{tag} OpenAI flagged: {i}")


# ── STATE (change detection) ──────────────────────────────────────────────────
_state    = {}
_state_lk = threading.Lock()


def load_state():
    global _state
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            _state = json.load(f)
        log.info(f"Loaded state: {len(_state)} entries")
    except FileNotFoundError:
        _state = {}
    except Exception as e:
        log.warning(f"State load failed ({e}) — starting empty")
        _state = {}


def save_state():
    with _state_lk:
        snapshot = dict(_state)
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snapshot, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.warning(f"State save failed: {e}")


def _hash_values(values) -> str:
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


# ── MIRROR ONE JOB ────────────────────────────────────────────────────────────
class Job:
    __slots__ = ("year", "month", "master_sid", "tab")

    def __init__(self, year, month, master_sid, tab):
        self.year, self.month, self.master_sid, self.tab = year, month, master_sid, tab

    @property
    def key(self):
        return f"{self.year}/{self.month}/{self.tab}"


def mirror_job(dsvc, ssvc, drive_id, job, args):
    tag = f"[{job.key}]"

    # 1) Cheap change check: master file modifiedTime (Drive quota, not Sheets).
    mtime = file_modified_time(dsvc, job.master_sid)
    with _state_lk:
        rec = _state.get(job.key, {})
    if (not args.force) and mtime and rec.get("src_mtime") == mtime \
            and rec.get("last_status") == "ok":
        log.debug(f"{tag} unchanged (mtime) — skip")
        return

    # 2) Read source tab.
    src = read_tab(ssvc, job.master_sid, job.tab)
    src_hash = _hash_values(src)
    if (not args.force) and rec.get("src_hash") == src_hash \
            and rec.get("last_status") == "ok":
        # Content identical (e.g. only formatting changed). Record mtime, skip write.
        with _state_lk:
            _state.setdefault(job.key, {}).update(src_mtime=mtime)
        log.debug(f"{tag} unchanged (hash) — skip write")
        return

    log.info(f"{tag} change detected — mirroring {len(src)} rows")
    if args.dry_run:
        warns = deterministic_schema_warnings(job.tab, src)
        for w in warns[:25]:
            log.info(f"{tag} (dry-run) schema: {w}")
        log.info(f"{tag} DRY-RUN: would write {len(src)} rows "
                 f"({len(warns)} schema warnings)")
        return

    # 3) Resolve destination spreadsheet path: copy-root / year / month / <tab>.
    dest_root = ensure_folder(dsvc, DEST_ROOT_FOLDER, drive_id, drive_id)
    yf        = ensure_folder(dsvc, job.year,  dest_root, drive_id)
    mf        = ensure_folder(dsvc, job.month, yf,        drive_id)
    dest_name = TAB_TO_DEST_SHEET[job.tab]
    dst_sid   = ensure_dest_spreadsheet(dsvc, ssvc, dest_name, job.tab, mf, drive_id)

    # 4) Mirror: write all source rows, then trim any trailing leftover rows.
    if src:
        write_tab(ssvc, dst_sid, job.tab, src)
    clear_below(ssvc, dst_sid, job.tab, keep_rows=len(src))

    # 5) Deterministic verify (authoritative integrity gate).
    dst = read_tab(ssvc, dst_sid, job.tab)
    ok, diff = _exact_equal(src, dst)
    status = "ok" if ok else "mismatch"
    if ok:
        log.info(f"{tag} verified OK ({len(src)} rows) → {dst_sid}")
    else:
        log.error(f"{tag} VERIFY MISMATCH: {diff}")

    # 6) Local schema warnings + OpenAI advisory validation (only on change).
    for w in deterministic_schema_warnings(job.tab, src)[:25]:
        log.warning(f"{tag} schema: {w}")
    try:
        openai_validate(job.tab, src, tag)
    except Exception as e:
        log.warning(f"{tag} OpenAI validation error (ignored): {e}")

    # 7) Persist state.
    with _state_lk:
        _state[job.key] = {
            "src_mtime":   mtime,
            "src_hash":    src_hash,
            "rows":        len(src),
            "dst_sid":     dst_sid,
            "last_status": status,
            "synced_at":   now_ist_str(),
        }
    save_state()


def _exact_equal(a, b):
    """Cell-by-cell compare of two ragged grids. Returns (equal, first_diff)."""
    if len(a) != len(b):
        return False, f"row count {len(a)} vs {len(b)}"
    for r, (ra, rb) in enumerate(zip(a, b), start=1):
        w = max(len(ra), len(rb))
        for c in range(w):
            va = ra[c] if c < len(ra) else ""
            vb = rb[c] if c < len(rb) else ""
            if (va or "") != (vb or ""):
                return False, f"row {r} col {c+1}: '{va}' != '{vb}'"
    return True, ""


# ── DISCOVERY ─────────────────────────────────────────────────────────────────
def discover_jobs(dsvc, drive_id, args):
    """Sweep source tree → one Job per (month, tab) that has a master sheet."""
    src_root = find_folder(dsvc, SOURCE_ROOT_FOLDER, drive_id, drive_id)
    if not src_root:
        log.error(f"Source root '{SOURCE_ROOT_FOLDER}' not found in drive")
        return []

    jobs = []
    for year, yid in list_subfolders(dsvc, src_root, drive_id):
        if not (year.isdigit() and len(year) == 4):
            continue
        if args.year and year != args.year:
            continue
        for month, mid in list_subfolders(dsvc, yid, drive_id):
            if month not in _MONTH_SET:
                continue
            if args.month and month != args.month:
                continue
            master_name = f"{month}_{year}"
            master_sid  = find_spreadsheet(dsvc, master_name, mid, drive_id)
            if not master_sid:
                log.debug(f"No master '{master_name}' in {year}/{month} — skip")
                continue
            for tab in TAB_TO_DEST_SHEET:
                jobs.append(Job(year, month, master_sid, tab))
    return jobs


# ── WORKER POOL ───────────────────────────────────────────────────────────────
def worker_loop(q: Queue, drive_id, args):
    """Persistent worker thread: builds its own Google clients, drains its queue."""
    dsvc = drive_svc()
    ssvc = sheets_svc()
    while True:
        job = q.get()
        try:
            mirror_job(dsvc, ssvc, drive_id, job, args)
        except Exception as e:
            log.exception(f"[{job.key}] mirror failed: {e}")
        finally:
            q.task_done()


def run_cycle(dsvc, drive_id, cand_q, is_q, args):
    jobs = discover_jobs(dsvc, drive_id, args)
    n_cand = sum(1 for j in jobs if j.tab == "Candidate")
    n_is   = len(jobs) - n_cand
    log.info(f"Discovered {len(jobs)} jobs ({n_cand} Candidate, {n_is} Interview-Success)")
    for j in jobs:
        (cand_q if j.tab == "Candidate" else is_q).put(j)
    cand_q.join()
    is_q.join()
    log.info("Cycle complete")


def main():
    ap = argparse.ArgumentParser(description="Mirror master month-sheets into 'Interview Success copy'")
    ap.add_argument("--once", action="store_true", help="run a single cycle then exit")
    ap.add_argument("--dry-run", action="store_true", help="discover + log, no writes")
    ap.add_argument("--no-openai", action="store_true", help="disable OpenAI validation")
    ap.add_argument("--force", action="store_true", help="ignore change-detection")
    ap.add_argument("--year", default="", help="restrict to one year, e.g. 2026")
    ap.add_argument("--month", default="", help="restrict to one month, e.g. May")
    args = ap.parse_args()

    global OPENAI_ENABLED
    if args.no_openai:
        OPENAI_ENABLED = False

    log.info("=" * 70)
    log.info("sheet_copy_worker starting")
    log.info(f"  source : {SHARED_DRIVE_NAME} / {SOURCE_ROOT_FOLDER} / <year> / <month> / <Month>_<Year>")
    log.info(f"  dest   : {SHARED_DRIVE_NAME} / {DEST_ROOT_FOLDER} / <year> / <month> / [Candidate | Interview-Success]")
    log.info(f"  workers: {CANDIDATE_WORKERS} Candidate + {IS_WORKERS} Interview-Success")
    log.info(f"  openai : {'ON (' + OPENAI_MODEL + ', advisory)' if OPENAI_ENABLED else 'OFF'}")
    log.info(f"  poll   : every {POLL_INTERVAL}s   mirror=exact   verify=deterministic")
    if args.dry_run:
        log.info("  MODE   : DRY-RUN (no writes)")
    log.info("=" * 70)

    load_state()
    dsvc     = drive_svc()
    drive_id = shared_drive_id(dsvc)

    cand_q: Queue = Queue()
    is_q:   Queue = Queue()
    for i in range(CANDIDATE_WORKERS):
        threading.Thread(target=worker_loop, args=(cand_q, drive_id, args),
                         name=f"cand-{i+1}", daemon=True).start()
    for i in range(IS_WORKERS):
        threading.Thread(target=worker_loop, args=(is_q, drive_id, args),
                         name=f"is-{i+1}", daemon=True).start()

    while True:
        start = time.time()
        try:
            run_cycle(dsvc, drive_id, cand_q, is_q, args)
        except Exception as e:
            log.exception(f"Cycle error: {e}")
        if args.once:
            break
        elapsed = time.time() - start
        time.sleep(max(POLL_INTERVAL - elapsed, 5))


if __name__ == "__main__":
    main()

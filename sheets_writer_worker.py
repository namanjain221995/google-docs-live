"""
sheets_writer_worker.py — FINAL VERSION
Handles ALL llm.txt formats:
  FORMAT 1 - JSON     : { "audit_summary_card": {...}, "audit_metadata": {...} }
  FORMAT 2 - Flat TOON: audit_summary_card,\n  candidate_name,John Doe\n  ...
  FORMAT 3 - Mixed    : Header text + either JSON or Flat TOON after ===
"""

import os, sys, json, time, re, logging, threading, queue, base64
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.config import Config
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from simple_salesforce import Salesforce

# ── CONFIG ────────────────────────────────────────────────────────────────────
AWS_REGION           = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET            = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
SF_SECRET_NAME       = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
API_SECRET_NAME      = os.environ.get("API_SECRET_NAME", "secrets/api")
SHARED_DRIVE_NAME    = "2026_Shared_Drive"
GDRIVE_FOLDER        = "Interview Success"
DEPARTMENTS          = ["Interview-Success", "Training", "Customer-Success", "Marketing"]
LIVE_WORKERS         = 10
BACKFILL_WORKERS     = 10
LIVE_POLL_INTERVAL   = 30
BACKFILL_INTERVAL    = 120
IST                  = timedelta(hours=5, minutes=30)
GOOGLE_SCOPES        = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# ── LOGGING ───────────────────────────────────────────────────────────────────
os.makedirs("/home/ec2-user/google-docs-live/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ec2-user/google-docs-live/logs/sheets_writer_worker.log"),
    ],
)
log = logging.getLogger("sheets_writer")

# ── RATE LIMITER: 45 writes / 60s across all threads ─────────────────────────
_RL_LIMIT = 45
_RL_WIN   = 60
_rl_lock  = threading.Lock()
_rl_times = []

def _token():
    while True:
        with _rl_lock:
            now = time.time()
            global _rl_times
            _rl_times = [t for t in _rl_times if t > now - _RL_WIN]
            if len(_rl_times) < _RL_LIMIT:
                _rl_times.append(now)
                return
            wait = _rl_times[0] + _RL_WIN - now + 0.1
        time.sleep(max(wait, 0.1))

# ── STATE ─────────────────────────────────────────────────────────────────────
live_q    = queue.Queue(maxsize=500)
_seen     = set()
_seen_lk  = threading.Lock()

# ── AWS ───────────────────────────────────────────────────────────────────────
_bcfg = Config(max_pool_connections=100)
s3c   = boto3.client("s3",             region_name=AWS_REGION, config=_bcfg)
smc   = boto3.client("secretsmanager", region_name=AWS_REGION)

_sec_cache = {}
_sec_lock  = threading.Lock()

def get_secret(name):
    with _sec_lock:
        if name not in _sec_cache:
            _sec_cache[name] = json.loads(smc.get_secret_value(SecretId=name)["SecretString"])
        return _sec_cache[name]

# ── GOOGLE ────────────────────────────────────────────────────────────────────
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
        _gcreds = service_account.Credentials.from_service_account_info(sa, scopes=GOOGLE_SCOPES)
        return _gcreds

drive_svc_fn  = lambda: build("drive",  "v3", credentials=gcreds(), cache_discovery=False)
sheets_svc_fn = lambda: build("sheets", "v4", credentials=gcreds(), cache_discovery=False)

# ── SALESFORCE ────────────────────────────────────────────────────────────────
_sf    = None
_sf_lk = threading.Lock()

def get_sf():
    global _sf
    with _sf_lk:
        if _sf:
            return _sf
        c   = get_secret(SF_SECRET_NAME)
        pk  = base64.b64decode(c["PRIVATE_KEY_B64"]).decode()
        _sf = Salesforce(username=c["SF_USERNAME"], consumer_key=c["SF_CLIENT_ID"],
                         privatekey=pk, domain="login")
        return _sf

def query_sf(mid):
    try:
        res = get_sf().query(
            f"SELECT Name,Candidate_Name__c,Date_of_Interview__c,Round_Info__c,Round__c "
            f"FROM Interview__c WHERE Zoom_Meeting_Id__c='{mid}' LIMIT 1"
        )
        recs = res.get("records", [])
        if not recs:
            log.warning(f"[{mid}] No SF record")
            return {}
        r = recs[0]
        return {
            "sf_id":   r.get("Name", ""),
            "name":    r.get("Candidate_Name__c", ""),
            "date":    r.get("Date_of_Interview__c", ""),
            "round":   r.get("Round_Info__c") or r.get("Round__c", ""),
        }
    except Exception as e:
        log.error(f"[{mid}] SF error: {e}")
        return {}

# ── S3 HELPERS ────────────────────────────────────────────────────────────────
def s3_read(key):
    try:
        return s3c.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
    except Exception:
        return ""

def s3_put_json(key, data):
    s3c.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(data, indent=2).encode(),
                   ContentType="application/json")

def find_llm_prefix(mid):
    """Scan all departments to find <mid>/llm/llm.txt and return base prefix."""
    pag = s3c.get_paginator("list_objects_v2")
    for dept in DEPARTMENTS:
        try:
            for page in pag.paginate(Bucket=S3_BUCKET, Prefix=f"{dept}/"):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    if mid in k and k.endswith("/llm/llm.txt"):
                        idx = k.find(f"/{mid}/")
                        if idx != -1:
                            return k[:idx + len(f"/{mid}")]
        except Exception as e:
            log.warning(f"Scan error {dept}: {e}")
    return ""

# ── S3 SCANNER ────────────────────────────────────────────────────────────────
def scan_s3(since=None):
    pag        = s3c.get_paginator("list_objects_v2")
    has_llm    = {}
    has_sheets = set()

    for page in pag.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key  = obj["Key"]
            lm   = obj.get("LastModified")
            parts = key.split("/")

            # NEW: temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/meeting_id/file
            if (len(parts) >= 7
                    and parts[2].isdigit() and len(parts[2]) == 4
                    and parts[3].startswith("Month-")):
                mid, fn, pfx = parts[5], parts[6], "/".join(parts[:6])

            # OLD: temp/live-doc-history/meeting_id/file
            elif len(parts) == 4 and parts[2].isdigit():
                mid, fn, pfx = parts[2], parts[3], "/".join(parts[:3])

            else:
                continue

            if not mid.isdigit():
                continue

            if fn == "llm-done.json":
                if since is None or (lm and lm > since):
                    has_llm[mid] = {"pfx": pfx, "lm": lm}
            elif fn == "sheets-done.json":
                has_sheets.add(mid)

    pending = [
        {"mid": mid, "pfx": info["pfx"], "lm": info["lm"]}
        for mid, info in has_llm.items()
        if mid not in has_sheets
    ]
    pending.sort(key=lambda x: x["lm"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    log.info(f"Scanner: {len(pending)} to process "
             f"({len(has_llm)} llm-done, {len(has_sheets)} sheets-done)")
    return pending

# ══════════════════════════════════════════════════════════════════════════════
# LLM OUTPUT PARSER — handles ALL formats
# ══════════════════════════════════════════════════════════════════════════════

# ── Format 1: JSON ────────────────────────────────────────────────────────────
def _try_json(txt):
    """
    Try to parse JSON format:
    { "audit_summary_card": { "candidate_name": ..., ... }, "audit_metadata": { ... } }
    Returns dict or None.
    """
    start = txt.find("{")
    if start == -1:
        return None
    try:
        data = json.loads(txt[start:])
    except Exception:
        try:
            data, _ = json.JSONDecoder().raw_decode(txt[start:])
        except Exception:
            return None

    sc = data.get("audit_summary_card", {})
    md = data.get("audit_metadata", {})
    cp = data.get("overall_candidate_performance", {})
    pp = data.get("overall_proxy_support_performance", {})
    vd = data.get("final_verdict", {})

    if not sc and not md:
        return None  # not the right JSON structure

    def join_cats(lst):
        if not isinstance(lst, list):
            return str(lst) if lst else ""
        return "; ".join(
            (x.get("category", str(x)) if isinstance(x, dict) else str(x))
            for x in lst
        )

    return {
        "candidate_name":                  sc.get("candidate_name") or md.get("candidate_detected", ""),
        "chance":                          str(sc.get("chance_of_moving_to_next_round_percent", "")),
        "candidate_action_required":       bool(sc.get("candidate_action_required", False)),
        "proxy_support_action_required":   bool(sc.get("proxy_support_action_required", False)),
        "candidate_action_categories":     join_cats(sc.get("candidate_action_categories", [])),
        "proxy_support_action_categories": join_cats(sc.get("proxy_support_action_categories", [])),
        "candidate_score":                 str(cp.get("score", "")),
        "proxy_score":                     str(pp.get("score", "")),
        "verdict":                         vd.get("one_line_verdict", ""),
        "round_type":                      md.get("round_type_detected", ""),
    }

# ── Format 2: Flat TOON (comma-separated key,value) ──────────────────────────
def _try_flat_toon(txt):
    """
    Parse Flat TOON format produced by Claude Haiku 4.5 / Bedrock:

    audit_metadata,
      round_type_detected,Recruiter Screen
      candidate_detected,John Doe (I-012345)

    audit_summary_card,
      candidate_name,John Doe
      chance_of_moving_to_next_round_percent,75
      candidate_action_required,true
      proxy_support_action_required,false
      candidate_action_categories[2],
        Communication / Delivery,reason text
        Confidence / Presence,reason text

    overall_candidate_performance,
      score,75

    one_line_verdict,"Strong candidate with minor gaps"

    Returns dict or None.
    """

    def get_field(text, field, default=""):
        """Match:  <field>[optional[N]],<value>"""
        pat = rf'^\s*{re.escape(field)}(?:\[\d+\])?\s*,\s*(.+?)\s*$'
        m = re.search(pat, text, re.MULTILINE | re.IGNORECASE)
        return m.group(1).strip().strip('"') if m else default

    def get_bool(text, field):
        return get_field(text, field).lower() in ("true", "yes", "1")

    def get_cats(text, field):
        """
        Extract sub-items from an array field:
          field[N],
            Category Name,reason...
            Category Name,reason...
        Stops when indent returns to header level or less.
        """
        pat = rf'^\s*{re.escape(field)}(?:\[\d+\])?\s*,\s*$'
        m   = re.search(pat, text, re.MULTILINE | re.IGNORECASE)
        if not m:
            return ""
        # Measure header indent
        line_start    = text.rfind("\n", 0, m.start()) + 1
        header_line   = text[line_start:m.end()]
        header_indent = len(header_line) - len(header_line.lstrip())
        cats = []
        for line in text[m.end():].split("\n"):
            if not line.strip():
                continue
            line_indent = len(line) - len(line.lstrip())
            if line_indent <= header_indent:
                break                              # back to same or outer level
            stripped = line.strip()
            if re.match(r'.+\[\d+\]\s*,\s*$', stripped):
                break                              # another array header — stop
            idx = stripped.find(",")
            if idx > 0:
                cat = stripped[:idx].strip().strip('"')
                if cat and not cat.isdigit():
                    cats.append(cat)
        return "; ".join(cats)

    # Must have at least one key,value comma-separated line to qualify
    if not re.search(r'^\s*\w[\w\s]*,\s*\S', txt, re.MULTILINE):
        return None

    candidate_name = (get_field(txt, "candidate_name")
                      or get_field(txt, "candidate_detected"))
    # Strip SF ID suffix like (I-025513)
    candidate_name = re.sub(r'\s*\([A-Z]-\d+\)', '', candidate_name).strip()

    chance = (get_field(txt, "chance_of_moving_to_next_round_percent")
              or get_field(txt, "probability_percent"))

    cand_action  = get_bool(txt, "candidate_action_required")
    proxy_action = get_bool(txt, "proxy_support_action_required")

    cac = get_cats(txt, "candidate_action_categories")
    pac = get_cats(txt, "proxy_support_action_categories")

    # Scores: first two occurrences of "score,<number>"
    scores      = re.findall(r'^\s*score\s*,\s*(\d+)', txt, re.MULTILINE)
    cand_score  = scores[0] if len(scores) > 0 else ""
    proxy_score = scores[1] if len(scores) > 1 else ""

    verdict    = get_field(txt, "one_line_verdict")
    round_type = get_field(txt, "round_type_detected")

    # Return None if we got nothing useful
    if not candidate_name and not chance and not verdict:
        return None

    return {
        "candidate_name":                  candidate_name,
        "chance":                          chance,
        "candidate_action_required":       cand_action,
        "proxy_support_action_required":   proxy_action,
        "candidate_action_categories":     cac,
        "proxy_support_action_categories": pac,
        "candidate_score":                 cand_score,
        "proxy_score":                     proxy_score,
        "verdict":                         verdict,
        "round_type":                      round_type,
    }

# ── Master parser: tries all formats ─────────────────────────────────────────
def parse_llm(llm_txt):
    """
    Auto-detect format and parse.
    Returns a flat dict with guaranteed keys, or {} if nothing works.
    Never raises.
    """
    if not llm_txt or not llm_txt.strip():
        log.warning("llm.txt is empty")
        return {}

    # Strip the report header (everything before the first blank line after ===)
    # The header looks like:
    #   LLM ANALYSIS REPORT
    #   Meeting ID: ...
    #   ...
    #   ============================================================
    #   <actual content>
    body = llm_txt
    sep_match = re.search(r'={4,}\s*\n', llm_txt)
    if sep_match:
        body = llm_txt[sep_match.end():]

    # ── Try JSON ──────────────────────────────────────────────────────────────
    result = _try_json(body)
    if result and result.get("candidate_name"):
        log.info(f"Format=JSON ✅  candidate='{result['candidate_name']}'  "
                 f"chance='{result['chance']}'  "
                 f"cand_action={result['candidate_action_required']}  "
                 f"proxy_action={result['proxy_support_action_required']}")
        return result

    # Also try JSON on the full text (in case no separator)
    result = _try_json(llm_txt)
    if result and result.get("candidate_name"):
        log.info(f"Format=JSON(full) ✅  candidate='{result['candidate_name']}'  "
                 f"chance='{result['chance']}'")
        return result

    # ── Try Flat TOON ─────────────────────────────────────────────────────────
    result = _try_flat_toon(body)
    if result:
        log.info(f"Format=FlatTOON ✅  candidate='{result['candidate_name']}'  "
                 f"chance='{result['chance']}'  "
                 f"cand_action={result['candidate_action_required']}  "
                 f"proxy_action={result['proxy_support_action_required']}  "
                 f"cac='{result['candidate_action_categories'][:80]}'  "
                 f"pac='{result['proxy_support_action_categories'][:80]}'")
        return result

    # Also try Flat TOON on full text
    result = _try_flat_toon(llm_txt)
    if result:
        log.info(f"Format=FlatTOON(full) ✅  candidate='{result['candidate_name']}'  "
                 f"chance='{result['chance']}'")
        return result

    # ── Nothing worked ────────────────────────────────────────────────────────
    log.warning(f"Could not parse llm.txt — unknown format. "
                f"First 400 chars:\n{llm_txt[:400]}")
    return {}

# ── Path helpers ──────────────────────────────────────────────────────────────
def _clean(s): return s.replace("_", " ").strip()

def extract_is_person(bp):
    parts = bp.split("/")
    return _clean(parts[1]) if len(parts) >= 2 and parts[0] in DEPARTMENTS else ""

def extract_year_month(bp):
    parts = bp.split("/")
    year = month_num = ""
    for p in parts:
        if p.isdigit() and len(p) == 4:
            year = p
        elif p.startswith("Month-"):
            try: month_num = int(p.replace("Month-", ""))
            except: pass
    names = ["","January","February","March","April","May","June",
             "July","August","September","October","November","December"]
    mn = names[month_num] if isinstance(month_num, int) and 0 < month_num <= 12 else "Unknown"
    return year, mn

# ── Google Drive cache + race-safe create ─────────────────────────────────────
_dc    = {}
_dc_lk = threading.Lock()
_cl    = {}
_cl_lk = threading.Lock()

def _lk(key):
    with _cl_lk:
        if key not in _cl: _cl[key] = threading.Lock()
        return _cl[key]

def _shared_drive(dsvc):
    k = "__sd__"
    with _dc_lk:
        if k in _dc: return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc: return _dc[k]
        for d in dsvc.drives().list(pageSize=20).execute().get("drives", []):
            if d["name"] == SHARED_DRIVE_NAME:
                with _dc_lk: _dc[k] = d["id"]
                return d["id"]
        raise ValueError(f"Shared drive '{SHARED_DRIVE_NAME}' not found")

def _folder(dsvc, name, parent, drive_id):
    k = f"f:{parent}:{name}"
    with _dc_lk:
        if k in _dc: return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc: return _dc[k]
        resp = dsvc.files().list(
            q=(f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
               f"and '{parent}' in parents and trashed=false"),
            spaces="drive", fields="files(id,name)",
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            corpora="drive", driveId=drive_id,
        ).execute()
        files = resp.get("files", [])
        if files:
            fid = files[0]["id"]
            if len(files) > 1: log.warning(f"{len(files)} folders '{name}' — using first")
        else:
            fid = dsvc.files().create(
                body={"name": name, "mimeType": "application/vnd.google-apps.folder",
                      "parents": [parent]},
                fields="id", supportsAllDrives=True,
            ).execute()["id"]
            log.info(f"✅ Created folder '{name}' → {fid}")
        with _dc_lk: _dc[k] = fid
        return fid

def _sheet(dsvc, ssvc, name, parent, drive_id):
    k = f"s:{parent}:{name}"
    with _dc_lk:
        if k in _dc: return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc: return _dc[k]
        resp = dsvc.files().list(
            q=(f"name='{name}' and mimeType='application/vnd.google-apps.spreadsheet' "
               f"and '{parent}' in parents and trashed=false"),
            spaces="drive", fields="files(id,name)",
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            corpora="drive", driveId=drive_id,
        ).execute()
        files = resp.get("files", [])
        if files:
            sid = files[0]["id"]
            log.info(f"Found sheet '{name}': {sid}")
        else:
            sid = dsvc.files().create(
                body={"name": name, "mimeType": "application/vnd.google-apps.spreadsheet",
                      "parents": [parent]},
                fields="id", supportsAllDrives=True,
            ).execute()["id"]
            log.info(f"✅ Created sheet '{name}': {sid}")
            _setup_tabs(ssvc, sid)
        with _dc_lk: _dc[k] = sid
        return sid

# ── Sheet tab setup ───────────────────────────────────────────────────────────
C_HDR = ["Date","Salesforce Interview ID","Candidate Name","Meeting ID",
         "Chance of Moving to Next Round %","Action Required","Candidate Action Categories"]
I_HDR = ["Date","Salesforce Interview ID","Interview-Success Person","Meeting ID",
         "Chance of Moving to Next Round %","Action Required","Proxy Support Action Categories"]
D_HDR = ["Date","Salesforce Interview ID","Candidate Name","Interview-Success Person",
         "Meeting ID","Chance of Moving to Next Round %",
         "Candidate Action Required","Proxy Support Action Required",
         "Candidate Action Categories","Proxy Support Action Categories",
         "Candidate Score","Proxy Score","Verdict","Round Type"]

def _setup_tabs(ssvc, sid):
    meta  = ssvc.spreadsheets().get(spreadsheetId=sid).execute()
    exist = meta.get("sheets", [])
    titles = [s["properties"]["title"] for s in exist]
    reqs = []
    if exist and titles[0] != "Candidate":
        reqs.append({"updateSheetProperties": {
            "properties": {"sheetId": exist[0]["properties"]["sheetId"], "title": "Candidate"},
            "fields": "title"}})
    for t in ["Interview-Success", "Data"]:
        if t not in titles:
            reqs.append({"addSheet": {"properties": {"title": t}}})
    if reqs:
        ssvc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": reqs}).execute()
    ssvc.spreadsheets().values().batchUpdate(
        spreadsheetId=sid,
        body={"valueInputOption": "RAW", "data": [
            {"range": "'Candidate'!A1",         "values": [C_HDR]},
            {"range": "'Interview-Success'!A1", "values": [I_HDR]},
            {"range": "'Data'!A1",              "values": [D_HDR]},
        ]},
    ).execute()
    log.info(f"Tabs set up for {sid}")

# ── Append row: rate-limited + backoff retry ──────────────────────────────────
def _append(ssvc, sid, tab, row):
    for attempt in range(6):
        _token()
        try:
            ssvc.spreadsheets().values().append(
                spreadsheetId=sid, range=f"'{tab}'!A1",
                valueInputOption="RAW", insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            ).execute()
            return
        except HttpError as e:
            if e.resp.status == 429:
                wait = (2 ** attempt) * 5
                log.warning(f"429 tab='{tab}' attempt {attempt+1}/6 sleep {wait}s")
                time.sleep(wait)
                if attempt == 5: raise
            else:
                log.error(f"Sheets error tab='{tab}': {e}")
                raise

# ── Core processor ────────────────────────────────────────────────────────────
def process(item):
    mid = item["mid"]
    pfx = item["pfx"]
    log.info(f"[{mid}] ── Processing ──")

    # 1. Read llm-done.json
    raw = s3_read(f"{pfx}/llm-done.json")
    if not raw:
        return f"SKIP {mid} — no llm-done.json"
    try:
        llm_done = json.loads(raw)
    except Exception:
        return f"SKIP {mid} — llm-done.json parse error"

    bp = llm_done.get("base_prefix", "").replace(f"s3://{S3_BUCKET}/", "").rstrip("/")
    log.info(f"[{mid}] base_prefix='{bp}'")

    # 2. Fallback: read done.json
    if not bp:
        done_raw = s3_read(f"{pfx}/done.json")
        if done_raw:
            try:
                bp = json.loads(done_raw).get("base_prefix", "").replace(
                    f"s3://{S3_BUCKET}/", "").rstrip("/")
                if bp: log.info(f"[{mid}] base_prefix from done.json: '{bp}'")
            except Exception:
                pass

    # 3. Find llm.txt
    llm_txt = llm_key = ""
    if bp:
        candidate = f"{bp}/llm/llm.txt"
        llm_txt = s3_read(candidate)
        if llm_txt:
            llm_key = candidate
            log.info(f"[{mid}] Found llm.txt: {llm_key}")

    if not llm_txt:
        log.info(f"[{mid}] Scanning S3 for llm.txt...")
        found = find_llm_prefix(mid)
        if found:
            bp      = found
            llm_key = f"{bp}/llm/llm.txt"
            llm_txt = s3_read(llm_key)
            if llm_txt:
                log.info(f"[{mid}] Found via scan: {llm_key}")

    if not llm_txt:
        return f"SKIP {mid} — llm.txt not found"

    # 4. Parse LLM output (handles ALL formats)
    parsed = parse_llm(llm_txt)
    if not parsed:
        return f"SKIP {mid} — could not parse llm.txt"

    # 5. Salesforce
    sf          = query_sf(mid)
    sf_id       = sf.get("sf_id", "")
    cand_name   = sf.get("name") or parsed.get("candidate_name", "")
    date_str    = sf.get("date") or (datetime.now(timezone.utc) + IST).strftime("%Y-%m-%d")

    # 6. Path metadata
    is_person       = extract_is_person(bp)
    year, month_name = extract_year_month(bp)
    if not year:
        now_ist    = datetime.now(timezone.utc) + IST
        year       = str(now_ist.year)
        month_name = now_ist.strftime("%B")
    log.info(f"[{mid}] is_person='{is_person}' year={year} month={month_name}")

    # 7. Google Drive navigation
    dsvc = drive_svc_fn()
    ssvc = sheets_svc_fn()
    did  = _shared_drive(dsvc)
    isf  = _folder(dsvc, GDRIVE_FOLDER, did,  did)
    yf   = _folder(dsvc, year,          isf,  did)
    mf   = _folder(dsvc, month_name,    yf,   did)
    sname = f"{month_name}_{year}"
    sid   = _sheet(dsvc, ssvc, sname, mf, did)

    # 8. Row values
    yn           = lambda f: "Yes" if f else "No"
    cand_action  = parsed.get("candidate_action_required", False)
    proxy_action = parsed.get("proxy_support_action_required", False)
    chance       = parsed.get("chance", "")
    cac          = parsed.get("candidate_action_categories", "")
    pac          = parsed.get("proxy_support_action_categories", "")
    c_score      = parsed.get("candidate_score", "")
    p_score      = parsed.get("proxy_score", "")
    verdict      = parsed.get("verdict", "")
    round_type   = parsed.get("round_type", "")

    # Routing
    write_c = cand_action  or (not cand_action and not proxy_action)
    write_i = proxy_action or (not cand_action and not proxy_action)

    # 9. Data tab — ALWAYS
    _append(ssvc, sid, "Data", [
        date_str, sf_id, cand_name, is_person, mid, str(chance),
        yn(cand_action), yn(proxy_action),
        cac, pac, c_score, p_score, verdict, round_type,
    ])
    log.info(f"[{mid}] ✅ Data written")

    # 10. Candidate tab
    if write_c:
        _append(ssvc, sid, "Candidate", [
            date_str, sf_id, cand_name, mid, str(chance), yn(cand_action), cac,
        ])
        log.info(f"[{mid}] ✅ Candidate written")

    # 11. Interview-Success tab
    if write_i:
        _append(ssvc, sid, "Interview-Success", [
            date_str, sf_id, is_person, mid, str(chance), yn(proxy_action), pac,
        ])
        log.info(f"[{mid}] ✅ Interview-Success written")

    # 12. Mark done
    now = datetime.now(timezone.utc)
    s3_put_json(f"{pfx}/sheets-done.json", {
        "meeting_id":       mid,
        "status":           "sheets_written",
        "processed_at":     now.isoformat(),
        "processed_at_ist": (now + IST).strftime("%Y-%m-%d %I:%M:%S %p IST"),
        "spreadsheet_id":   sid,
        "sheet_name":       sname,
        "llm_key":          llm_key,
        "base_prefix":      bp,
        "tabs_written":     {"Data": True, "Candidate": write_c, "Interview-Success": write_i},
        "routing_reason":   ("both_true"      if (cand_action and proxy_action) else
                             "candidate_only" if cand_action else
                             "proxy_only"     if proxy_action else
                             "both_false"),
        "sf_interview_id":  sf_id,
        "candidate_name":   cand_name,
        "is_person":        is_person,
    })
    log.info(f"[{mid}] ✅ sheets-done.json written")
    return f"OK {mid} → {sname} | {sid}"

# ── Live poller ───────────────────────────────────────────────────────────────
def live_poller():
    log.info("Live poller started")
    last = datetime.now(timezone.utc) - timedelta(minutes=10)
    while True:
        try:
            items = scan_s3(since=last)
            last  = datetime.now(timezone.utc)
            new   = 0
            for item in items:
                with _seen_lk:
                    if item["mid"] not in _seen:
                        _seen.add(item["mid"])
                        try: live_q.put_nowait(item); new += 1
                        except queue.Full: log.warning(f"Queue full — dropping {item['mid']}")
            if new: log.info(f"Live poller: queued {new}")
        except Exception as e:
            log.error(f"Live poller error: {e}", exc_info=True)
        time.sleep(LIVE_POLL_INTERVAL)

# ── Live workers ──────────────────────────────────────────────────────────────
def live_worker():
    log.info("Live worker ready")
    while True:
        try:
            item = live_q.get(timeout=60)
            try:
                log.info(f"[LIVE] {process(item)}")
            except Exception as e:
                log.error(f"[LIVE] Error {item['mid']}: {e}", exc_info=True)
            finally:
                live_q.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            log.error(f"Live worker error: {e}", exc_info=True)

# ── Backfill loop ─────────────────────────────────────────────────────────────
def backfill_loop():
    log.info("Backfill loop started")
    time.sleep(15)
    while True:
        try:
            pending = []
            for item in scan_s3():
                with _seen_lk:
                    if item["mid"] not in _seen:
                        _seen.add(item["mid"])
                        pending.append(item)

            if not pending:
                log.info(f"Backfill: nothing new. Sleeping {BACKFILL_INTERVAL}s")
                time.sleep(BACKFILL_INTERVAL)
                continue

            log.info(f"Backfill: processing {len(pending)} meetings")
            with ThreadPoolExecutor(max_workers=BACKFILL_WORKERS,
                                    thread_name_prefix="backfill") as ex:
                futures = {ex.submit(process, item): item["mid"] for item in pending}
                for future in as_completed(futures):
                    mid = futures[future]
                    try:
                        log.info(f"[BACKFILL] {future.result()}")
                    except Exception as e:
                        log.error(f"[BACKFILL] Error {mid}: {e}", exc_info=True)

            log.info(f"Backfill done. Sleeping {BACKFILL_INTERVAL}s")
            time.sleep(BACKFILL_INTERVAL)
        except Exception as e:
            log.error(f"Backfill error: {e}", exc_info=True)
            time.sleep(30)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("sheets_writer_worker starting")
    log.info("=" * 60)

    try:
        gcreds()
        log.info("Google credentials ✅")
    except Exception as e:
        log.error(f"Google creds failed: {e}"); sys.exit(1)

    try:
        get_sf()
        log.info("Salesforce ✅")
    except Exception as e:
        log.error(f"Salesforce failed: {e}"); sys.exit(1)

    try:
        sd_id = _shared_drive(drive_svc_fn())
        log.info(f"Shared drive '{SHARED_DRIVE_NAME}': {sd_id} ✅")
    except Exception as e:
        log.error(f"Shared drive not found: {e}"); sys.exit(1)

    threading.Thread(target=live_poller,   daemon=True, name="live-poller").start()
    threading.Thread(target=backfill_loop, daemon=True, name="backfill-loop").start()

    threads = []
    for i in range(LIVE_WORKERS):
        t = threading.Thread(target=live_worker, daemon=True, name=f"live-{i+1}")
        t.start(); threads.append(t)

    log.info(f"All workers started — {LIVE_WORKERS} live + {BACKFILL_WORKERS} backfill")

    while True:
        time.sleep(60)
        alive = sum(1 for t in threads if t.is_alive())
        log.info(f"Heartbeat — live={alive} queue={live_q.qsize()}")

if __name__ == "__main__":
    main()
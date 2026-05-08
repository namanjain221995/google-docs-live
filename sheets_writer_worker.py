"""
sheets_writer_worker.py — BULLETPROOF VERSION
================================================
Handles ALL llm.txt output formats:

  FORMAT 1 — Flat TOON (Claude Haiku 4.5 / Bedrock):
    audit_summary_card,
      candidate_name,John Doe
      chance_of_moving_to_next_round_percent,75
      candidate_action_required,true
      candidate_action_categories[2],
        Technical Depth,reason text
      candidate_action_categories[1]{category,"Technical Depth",reason,"..."}

  FORMAT 2 — JSON (OpenAI GPT-4o-mini):
    { "audit_summary_card": { "candidate_name": "...", ... } }

  FORMAT 3 — Mixed:
    Header + separator (====) + either JSON or Flat TOON

Parser tries every known pattern variant so no matter how the LLM
formats the output, we always extract:
  candidate_name, chance, candidate_action_required,
  proxy_support_action_required, candidate_action_categories,
  proxy_support_action_categories, candidate_score, proxy_score,
  verdict, round_type
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
AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET         = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
SF_SECRET_NAME    = os.environ.get("SF_SECRET_NAME", "sf/jwt/credentials")
API_SECRET_NAME   = os.environ.get("API_SECRET_NAME", "secrets/api")
SHARED_DRIVE_NAME = "2026_Shared_Drive"
GDRIVE_FOLDER     = "Interview Success"
DEPARTMENTS       = ["Interview-Success", "Training", "Customer-Success", "Marketing"]
LIVE_WORKERS      = 10
BACKFILL_WORKERS  = 5   # Reduced to avoid Sheets 429 rate limit

# ONLY these 3 people should have access — all others will be removed
SHARE_WITH_EMAILS = [
    "naman.jain@techsarasolutions.com",
    "rajvi.patel@techsarasolutions.com",
    "sahil.patel@techsarasolutions.com",
]
LIVE_POLL_INTERVAL   = 30
BACKFILL_INTERVAL    = 120
IST               = timedelta(hours=5, minutes=30)
GOOGLE_SCOPES     = [
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
        logging.FileHandler(
            "/home/ec2-user/google-docs-live/logs/sheets_writer_worker.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("sheets_writer")

# ── RATE LIMITER: 45 writes / 60s ─────────────────────────────────────────────
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
live_q   = queue.Queue(maxsize=500)
_seen    = set()
_seen_lk = threading.Lock()

# ── AWS ───────────────────────────────────────────────────────────────────────
_bcfg = Config(max_pool_connections=100)
s3c   = boto3.client("s3",             region_name=AWS_REGION, config=_bcfg)
smc   = boto3.client("secretsmanager", region_name=AWS_REGION)

_sec_cache = {}
_sec_lock  = threading.Lock()

def get_secret(name):
    with _sec_lock:
        if name not in _sec_cache:
            _sec_cache[name] = json.loads(
                smc.get_secret_value(SecretId=name)["SecretString"]
            )
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
        _gcreds = service_account.Credentials.from_service_account_info(
            sa, scopes=GOOGLE_SCOPES
        )
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
        _sf = Salesforce(
            username=c["SF_USERNAME"],
            consumer_key=c["SF_CLIENT_ID"],
            privatekey=pk,
            domain="login",
        )
        return _sf

def query_sf(mid):
    try:
        res = get_sf().query(
            f"SELECT Name,Candidate_Name__c,Date_of_Interview__c,"
            f"Round_Info__c,Round__c "
            f"FROM Interview__c WHERE Zoom_Meeting_Id__c='{mid}' LIMIT 1"
        )
        recs = res.get("records", [])
        if not recs:
            log.warning(f"[{mid}] No SF record")
            return {}
        r = recs[0]
        return {
            "sf_id": r.get("Name", ""),
            "name":  r.get("Candidate_Name__c", ""),
            "date":  r.get("Date_of_Interview__c", ""),
            "round": r.get("Round_Info__c") or r.get("Round__c", ""),
        }
    except Exception as e:
        log.error(f"[{mid}] SF error: {e}")
        return {}

# ── S3 HELPERS ────────────────────────────────────────────────────────────────
def s3_read(key):
    try:
        return s3c.get_object(
            Bucket=S3_BUCKET, Key=key
        )["Body"].read().decode("utf-8")
    except Exception:
        return ""

def s3_put_json(key, data):
    s3c.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(data, indent=2).encode(),
        ContentType="application/json",
    )

def find_llm_key(mid):
    """Scan all departments to find <mid>/llm/llm.txt. Returns S3 key or ''."""
    pag = s3c.get_paginator("list_objects_v2")
    for dept in DEPARTMENTS:
        try:
            for page in pag.paginate(Bucket=S3_BUCKET, Prefix=f"{dept}/"):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    if mid in k and k.endswith("/llm/llm.txt"):
                        return k
        except Exception as e:
            log.warning(f"Scan error {dept}: {e}")
    return ""

def find_base_prefix_from_llm_key(k, mid):
    """Extract base prefix from llm key: strip /llm/llm.txt"""
    if k.endswith("/llm/llm.txt"):
        return k[: -len("/llm/llm.txt")]
    return ""

# ── S3 SCANNER ────────────────────────────────────────────────────────────────
def scan_s3(since=None):
    pag        = s3c.get_paginator("list_objects_v2")
    has_llm    = {}
    has_sheets = set()

    for page in pag.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key   = obj["Key"]
            lm    = obj.get("LastModified")
            parts = key.split("/")

            # NEW: temp/live-doc-history/YYYY/Month-M/YYYY-MM-DD/mid/file
            if (len(parts) >= 7
                    and parts[2].isdigit() and len(parts[2]) == 4
                    and parts[3].startswith("Month-")):
                mid, fn, pfx = parts[5], parts[6], "/".join(parts[:6])

            # OLD: temp/live-doc-history/mid/file
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
    pending.sort(
        key=lambda x: x["lm"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    log.info(
        f"Scanner: {len(pending)} to process "
        f"({len(has_llm)} llm-done, {len(has_sheets)} sheets-done)"
    )
    return pending

# ══════════════════════════════════════════════════════════════════════════════
# BULLETPROOF LLM OUTPUT PARSER
# ══════════════════════════════════════════════════════════════════════════════

def _strip_quotes(s: str) -> str:
    """Remove surrounding quotes from a value string."""
    s = s.strip()
    if len(s) >= 2 and s[0] in ('"', "'") and s[-1] == s[0]:
        return s[1:-1].strip()
    return s


def _to_bool(s: str) -> bool:
    return str(s).lower().strip() in ("true", "yes", "1", "t", "y")


def _extract_categories_flat_toon(text: str, field_name: str) -> str:
    """
    Extract category names. Handles ALL known formats:

    FORMAT A — inline brace with real category value:
      field[1]{category,"Technical Depth",reason,"..."}

    FORMAT B — {category,reason} SCHEMA HEADER then CSV lines:
      (with OR without trailing comma after closing brace)
      field[3]{category,reason},     <- with comma
      field[3]{category,reason}      <- without comma (also supported)
        Communication / Delivery,"reason text"
        Confidence / Presence,"reason text"

    FORMAT C — [N], array header then indented lines:
      field[2],
        Technical Depth,reason text
        Communication / Delivery,reason text

    FORMAT D — category sub-field inside block:
      field[1],
        -
          category,Technical Depth
    """
    cats = []
    SCHEMA_WORDS = {
        'reason', 'why', 'explanation', 'priority', 'problem_observed',
        'what_to_do_next_time', 'example_better_response_or_behavior',
        'problem', 'action', 'fix', 'severity', 'impact', 'timestamp',
        'document_version_or_phase', 'category', 'name', 'type',
    }

    # ── FORMAT A: {category,"Real Name",reason,"..."} ────────────────────────
    pat_a = re.compile(
        rf'{re.escape(field_name)}\[\d+\]\{{[^}}]*?category\s*,\s*"?([^",\}}]+)"?',
        re.IGNORECASE,
    )
    for m in pat_a.finditer(text):
        val = m.group(1).strip().strip('"').strip()
        if val and val.lower() not in SCHEMA_WORDS:
            cats.append(val)
    if cats:
        return "; ".join(dict.fromkeys(cats))

    # ── FORMAT B: {category,reason} SCHEMA HEADER then CSV data lines ────────
    # Matches WITH or WITHOUT trailing comma after }
    # field[N]{category,reason},   OR   field[N]{category,reason}
    pat_b = re.compile(
        rf'^\s*{re.escape(field_name)}(?:\[\d+\])?\{{[^}}]*category[^}}]*reason[^}}]*\}}\s*,?\s*$',
        re.IGNORECASE | re.MULTILINE,
    )
    m_b = pat_b.search(text)
    if m_b:
        line_start    = text.rfind("\n", 0, m_b.start()) + 1
        header_line   = text[line_start: text.find("\n", m_b.start())]
        header_indent = len(header_line) - len(header_line.lstrip())
        after = text[m_b.end():]
        for line in after.split("\n"):
            if not line.strip():
                continue
            line_indent = len(line) - len(line.lstrip())
            if line_indent <= header_indent:
                break
            stripped = line.strip()
            if not stripped:
                continue
            # Extract first value before first unquoted comma
            # Line: Category Name,"reason text",
            first_comma = -1
            in_quote = False
            qchar = None
            for ci, ch in enumerate(stripped):
                if not in_quote and ch in ('"', "'"):
                    in_quote = True
                    qchar = ch
                elif in_quote and ch == qchar:
                    in_quote = False
                    qchar = None
                elif not in_quote and ch == ',':
                    first_comma = ci
                    break
            if first_comma > 0:
                cat = _strip_quotes(stripped[:first_comma])
            else:
                cat = _strip_quotes(stripped.rstrip(','))
            if cat and not cat.isdigit() and len(cat) > 1 and cat.lower() not in SCHEMA_WORDS:
                cats.append(cat)
        if cats:
            return "; ".join(dict.fromkeys(cats))

    # ── FORMAT C: [N], header then indented Name,reason lines ────────────────
    pat_c = re.compile(
        rf'^\s*{re.escape(field_name)}(?:\[\d+\])?\s*,\s*$',
        re.IGNORECASE | re.MULTILINE,
    )
    for m_c in pat_c.finditer(text):
        line_start    = text.rfind("\n", 0, m_c.start()) + 1
        header_line   = text[line_start: text.find("\n", m_c.start())]
        header_indent = len(header_line) - len(header_line.lstrip())
        after = text[m_c.end():]
        for line in after.split("\n"):
            if not line.strip():
                continue
            line_indent = len(line) - len(line.lstrip())
            if line_indent <= header_indent:
                break
            stripped = line.strip().lstrip("- ").strip()
            if not stripped or stripped.startswith("#"):
                continue
            if re.match(r'^(reason|why|explanation|details?|priority)\s*,', stripped, re.I):
                continue
            if re.match(r'^\w[\w\s]*\[\d+\]\s*[,{]', stripped):
                break
            idx = stripped.find(",")
            if idx > 0:
                cat = _strip_quotes(stripped[:idx])
            else:
                cat = _strip_quotes(stripped)
            if cat and not cat.isdigit() and len(cat) > 1 and cat.lower() not in SCHEMA_WORDS:
                cats.append(cat)
    if cats:
        return "; ".join(dict.fromkeys(cats))

    # ── FORMAT D: category,<Value> sub-field inside block ────────────────────
    block_pat = re.compile(
        rf'^\s*{re.escape(field_name)}(?:\[\d+\])?\s*[,{{]',
        re.IGNORECASE | re.MULTILINE,
    )
    for m_d in block_pat.finditer(text):
        after = text[m_d.end():]
        for i, line in enumerate(after.split("\n")):
            if i > 30:
                break
            cm = re.match(r'^\s*(?:-\s*)?category\s*,\s*(.+)$', line, re.IGNORECASE)
            if cm:
                val = _strip_quotes(cm.group(1))
                if val and val.lower() not in SCHEMA_WORDS:
                    cats.append(val)
    return "; ".join(dict.fromkeys(cats)) if cats else ""
def _get_field_flat_toon(text: str, field_name: str, default: str = "") -> str:
    """
    Extract a simple scalar value from flat TOON text.
    Tries multiple patterns to be bulletproof.
    """
    patterns = [
        # Standard: field_name,value  or  field_name,"value"
        rf'^\s*{re.escape(field_name)}\s*,\s*"?([^"\n,{{]+)"?\s*$',
        # With array index: field_name[N],value
        rf'^\s*{re.escape(field_name)}\[\d+\]\s*,\s*"?([^"\n,{{]+)"?\s*$',
        # Quoted: field_name,"value with spaces"
        rf'^\s*{re.escape(field_name)}\s*,\s*"([^"]+)"\s*$',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.MULTILINE | re.IGNORECASE)
        if m:
            v = m.group(1).strip().strip('"').strip("'")
            if v:
                return v
    return default


def _parse_flat_toon(body: str) -> dict | None:
    """
    Parse Flat TOON format. Returns dict or None if not enough data found.
    """
    # Must look like flat TOON
    if not re.search(r'^\s*\w[\w\s/]*,\s*\S', body, re.MULTILINE):
        return None

    # ── candidate_name ────────────────────────────────────────────────────────
    # Multi-part LLM outputs have multiple audit_summary_cards.
    # Part 1 may have "Unknown (Candidate ID: I-025621)" while Part 2/3 has real name.
    # Strategy: collect ALL candidate_name values, pick best (last non-Unknown/non-ID).
    def _best_name(text):
        names = re.findall(
            r'^\s*candidate_name\s*,\s*"?([^"\n]+)"?\s*$',
            text, re.MULTILINE | re.IGNORECASE
        )
        # Also try candidate_detected
        detected = re.findall(
            r'^\s*candidate_detected\s*,\s*"?([^"\n]+)"?\s*$',
            text, re.MULTILINE | re.IGNORECASE
        )
        all_names = names + detected
        if not all_names:
            return ""
        JUNK = ("unknown", "i-0", "candidate id", "not captured", "n/a", "none")
        # Pick last name that is not junk
        for n in reversed(all_names):
            n = n.strip().strip('"').strip()
            if n and not any(j in n.lower() for j in JUNK):
                return n
        # Fall back to first name, cleaned
        return all_names[0].strip().strip('"').strip()

    candidate_name = _best_name(body) or _best_name(text)
    # Strip SF ID suffix like (I-025513)
    candidate_name = re.sub(r'\s*\([A-Z]-\d+\)', '', candidate_name).strip()

    # ── chance ────────────────────────────────────────────────────────────────
    chance = (
        _get_field_flat_toon(body, "chance_of_moving_to_next_round_percent")
        or _get_field_flat_toon(body, "probability_percent")
        or _get_field_flat_toon(body, "chance_percent")
    )

    # ── action required flags ─────────────────────────────────────────────────
    cand_action  = _get_field_flat_toon(body, "candidate_action_required")
    proxy_action = _get_field_flat_toon(body, "proxy_support_action_required")

    # ── action categories ─────────────────────────────────────────────────────
    cac = _extract_categories_flat_toon(body, "candidate_action_categories")
    pac = _extract_categories_flat_toon(body, "proxy_support_action_categories")

    # ── scores (X/10 format) ──────────────────────────────────────────────────
    # First score = candidate, second = proxy
    scores      = re.findall(r'^\s*score\s*,\s*([\d.]+)', body, re.MULTILINE)
    cand_score  = (scores[0] + "/10") if len(scores) > 0 else ""
    proxy_score = (scores[1] + "/10") if len(scores) > 1 else ""

    # ── verdict ───────────────────────────────────────────────────────────────
    verdict = (
        _get_field_flat_toon(body, "one_line_verdict")
        or _get_field_flat_toon(body, "verdict")
    )

    # ── round_type ────────────────────────────────────────────────────────────
    round_type = (
        _get_field_flat_toon(body, "round_type_detected")
        or _get_field_flat_toon(body, "round_type")
    )

    # Reject if basically empty
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
        "round_type":                      round_type,
        "duration":                        duration,
        "total_questions":                 total_questions,
    }


def _parse_json(body: str) -> dict | None:
    """
    Parse JSON format. Returns dict or None.
    Tries multiple JSON variants and key names.
    """
    # Find first { in body
    start = body.find("{")
    if start == -1:
        return None

    data = None
    for attempt in [body[start:], body]:
        try:
            data = json.loads(attempt)
            break
        except Exception:
            try:
                data, _ = json.JSONDecoder().raw_decode(attempt)
                break
            except Exception:
                continue

    if not data or not isinstance(data, dict):
        return None

    # Try all possible key variants
    sc = (data.get("audit_summary_card")
          or data.get("summary_card")
          or data.get("audit_summary")
          or {})
    md = (data.get("audit_metadata")
          or data.get("metadata")
          or {})
    cp = (data.get("overall_candidate_performance")
          or data.get("candidate_performance")
          or {})
    pp = (data.get("overall_proxy_support_performance")
          or data.get("proxy_support_performance")
          or data.get("proxy_performance")
          or {})
    vd = (data.get("final_verdict")
          or data.get("verdict")
          or {})

    if not sc and not md:
        return None  # not our JSON structure

    # Extract categories from JSON
    def json_cats(lst):
        if not lst:
            return ""
        if isinstance(lst, str):
            return lst
        items = []
        for x in lst:
            if isinstance(x, dict):
                items.append(
                    x.get("category")
                    or x.get("name")
                    or x.get("type")
                    or str(x)
                )
            else:
                items.append(str(x))
        return "; ".join(i for i in items if i)

    candidate_name = (
        sc.get("candidate_name")
        or md.get("candidate_detected")
        or md.get("candidate")
        or ""
    )
    # Strip SF ID
    candidate_name = re.sub(r'\s*\([A-Z]-\d+\)', '', str(candidate_name)).strip()

    chance = str(
        sc.get("chance_of_moving_to_next_round_percent")
        or sc.get("chance_percent")
        or data.get("chance_of_moving_forward", {}).get("probability_percent", "")
        or ""
    )

    cand_action  = bool(sc.get("candidate_action_required", False))
    proxy_action = bool(sc.get("proxy_support_action_required", False))

    cac = json_cats(sc.get("candidate_action_categories", []))
    pac = json_cats(sc.get("proxy_support_action_categories", []))

    cand_score  = str(cp.get("score", ""))
    proxy_score = str(pp.get("score", ""))

    verdict = (
        vd.get("one_line_verdict")
        or vd.get("verdict")
        or str(vd) if isinstance(vd, str) else ""
    )
    round_type = (
        md.get("round_type_detected")
        or md.get("round_type")
        or ""
    )

    # ── Normalize duration to consistent format ────────────────────────────────
    if duration:
        d = duration.strip()
        dl = d.lower()
        # "1 hour 6 minutes" → "66 minutes"
        h_m = _re.match(r"(\d+)\s*hours?\s*(\d+)\s*min", dl)
        if h_m:
            duration = str(int(h_m.group(1)) * 60 + int(h_m.group(2))) + " minutes"
        # "1 hour" or "2 hours" → "60 minutes"
        elif _re.match(r"(\d+)\s*hours?\s*$", dl):
            h_m2 = _re.match(r"(\d+)", dl)
            duration = str(int(h_m2.group(1)) * 60) + " minutes"
        # "Approximately 45 minutes" / "approximately 45" → "45 minutes"
        elif _re.match(r"approx", dl):
            a_m = _re.search(r"(\d+)", dl)
            if a_m:
                duration = a_m.group(1) + " minutes"
        # "60 mins" → "60 minutes"
        elif _re.match(r"(\d+)\s*mins?\s*$", dl):
            mn_m = _re.match(r"(\d+)", dl)
            duration = mn_m.group(1) + " minutes"

    if not candidate_name and not chance:
        return None

    # ── Validate action_required against category content ────────────────────
    # If ALL categories say "No Action Needed" → override action_required to False
    NO_ACTION_PHRASES = ("no action needed", "no action required", "none", "n/a")

    if cac and all(
        any(p in cat.lower() for p in NO_ACTION_PHRASES)
        for cat in cac.split(";") if cat.strip()
    ):
        cand_action = False

    if pac and all(
        any(p in cat.lower() for p in NO_ACTION_PHRASES)
        for cat in pac.split(";") if cat.strip()
    ):
        proxy_action = False

    # Duration from flat toon
    duration        = _get_field_flat_toon(body, "total_transcript_duration") or                       _get_field_flat_toon(body, "session_duration")
    total_questions = _get_field_flat_toon(body, "total_questions_asked") or                       _get_field_flat_toon(body, "total_questions")
    total_pastes    = _get_field_flat_toon(body, "document_versions_count") or                       _get_field_flat_toon(body, "total_pastes")

    return {
        "candidate_name":                  candidate_name,
        "chance":                          chance,
        "candidate_action_required":       cand_action,
        "proxy_support_action_required":   proxy_action,
        "candidate_action_categories":     cac,
        "proxy_support_action_categories": pac,
        "candidate_score":                 cand_score,
        "proxy_score":                     proxy_score,
        "round_type":                      round_type,
        "duration":                        duration,
        "total_questions":                 total_questions,
    }


def _default_result():
    """Empty result with all required keys."""
    return {
        "candidate_name":                  "",
        "chance":                          "",
        "candidate_action_required":       "",
        "proxy_support_action_required":   "",
        "candidate_action_categories":     "",
        "proxy_support_action_categories": "",
        "candidate_score":                 "",
        "proxy_score":                     "",
        "round_type":                      "",
        "duration":                        "",
        "total_questions":                 "",
    }


def _parse_markdown_tables(text, log_ref=None):
    """
    Parse ALL GPT-4o-mini and Bedrock markdown table formats:
    - With title:  ## TABLE 1: PROXY ... / **Proxy Support Performance Table** / ### Table 1: ...
    - Without title: raw | Date | SF ID | ... | header row directly
    - Wrapped in ```markdown blocks
    - Vertical key-value format: | **Date** | 2026-05-01 |
    """
    import re as _re

    # Strip code fences
    text = _re.sub(r"```[a-zA-Z]*\n", "", text)
    text = _re.sub(r"```", "", text)

    # Must have pipe tables
    if "|" not in text:
        return None

    def clean(val):
        val = _re.sub(r"\*\*([^*]*)\*\*", r"\1", val)
        val = _re.sub(r"\*([^*]*)\*",   r"\1", val)
        return val.lstrip("~").strip()

    def is_sep(line):
        return bool(_re.match(r"^\|[-| :]+\|$", line.strip()))

    # Known header cell values — never treat these as data
    _HEADER_CELLS = {
        "date", "salesforce interview id", "interview-success person",
        "meeting id", "chance of moving to next round %", "chance %",
        "action required", "proxy support action categories",
        "candidate action categories", "candidate name", "sf id",
        "is person", "categories", "chance", "action",
    }

    def parse_table_at(lines, start_idx):
        """Given lines and index of header or title row, find data row."""
        i = start_idx
        found_sep = False
        while i < len(lines) and i < start_idx + 25:
            l = lines[i].strip()
            if not l:
                i += 1
                continue
            if is_sep(l):
                found_sep = True
                i += 1
                continue
            if l.startswith("|"):
                if found_sep:
                    cells = [clean(c.strip()) for c in l.split("|") if c.strip()]
                    if len(cells) >= 4:
                        # Reject if first cell looks like a header
                        if cells[0].lower().strip() in _HEADER_CELLS:
                            i += 1
                            continue
                        # Reject if looks like a VTT timestamp row (date col = HH:MM:SS)
                        if _re.match(r"^\d{2}:\d{2}(:\d{2})?$", cells[0].strip()):
                            i += 1
                            continue
                        return cells
            i += 1
        return None

    lines = text.split("\n")

    # Strategy 1: Find by TITLE keyword (## TABLE 1, **Proxy Support..., ### Table 1)
    def find_by_title(keyword):
        results = []
        for i, line in enumerate(lines):
            # Skip lines that are data rows (have dates)
            if _re.search(r"\d{4}-\d{2}-\d{2}", line):
                continue
            if _re.search(keyword, line, _re.IGNORECASE):
                row = parse_table_at(lines, i + 1)
                if row:
                    results.append(row)
        return results[-1] if results else None

    # Strategy 2: Find by COLUMN HEADER keywords
    def find_by_column_header(col_keywords):
        """Find table whose header row contains specific column keywords."""
        results = []
        for i, line in enumerate(lines):
            if not line.strip().startswith("|"):
                continue
            # Check if this line looks like a header (has our keywords)
            line_clean = clean(line).lower()
            if all(kw.lower() in line_clean for kw in col_keywords):
                # Check next line is separator
                if i + 1 < len(lines) and is_sep(lines[i + 1].strip()):
                    # Data is at i+2
                    if i + 2 < len(lines):
                        l2 = lines[i + 2].strip()
                        if l2.startswith("|"):
                            cells = [clean(c.strip()) for c in l2.split("|") if c.strip()]
                            if len(cells) >= 4:
                                results.append(cells)
        return results[-1] if results else None

    # Strategy 3: Vertical table (| **Key** | Value |)
    def find_vertical_table(key_map):
        """Parse | Key | Value | style tables."""
        result = {}
        for line in lines:
            if not line.strip().startswith("|"):
                continue
            cells = [clean(c.strip()) for c in line.split("|") if c.strip()]
            if len(cells) == 2:
                k, v = cells[0].lower(), cells[1]
                for field, aliases in key_map.items():
                    if any(a in k for a in aliases):
                        result[field] = v
        return result if len(result) >= 3 else None

    # ── Try all strategies for PROXY table ─────────────────────────────────
    proxy_cells = (
        find_by_title("PROXY") or
        find_by_column_header(["interview-success person", "proxy support action categories"]) or
        find_by_column_header(["interview-success person", "proxy support action required"])
    )

    # ── Try all strategies for CANDIDATE table ──────────────────────────────
    cand_cells = (
        find_by_title("CANDIDATE") or
        find_by_column_header(["candidate name", "candidate action categories"]) or
        find_by_column_header(["candidate name", "candidate action required"])
    )

    # ── Vertical format fallback ────────────────────────────────────────────
    vert = None
    if not proxy_cells and not cand_cells:
        vert = find_vertical_table({
            "date":           ["date"],
            "sf_id":          ["salesforce", "interview id"],
            "is_person":      ["interview-success person", "is person"],
            "meeting_id":     ["meeting id"],
            "chance":         ["chance"],
            "proxy_action":   ["proxy support action required"],
            "cand_action":    ["candidate action required"],
            "cac":            ["candidate action categories"],
            "pac":            ["proxy support action categories"],
            "candidate_name": ["candidate name"],
        })

    if not proxy_cells and not cand_cells and not vert:
        return None

    # ── Extract fields ──────────────────────────────────────────────────────
    if vert:
        pac            = vert.get("pac", "")
        proxy_action   = vert.get("proxy_action", "")
        candidate_name = vert.get("candidate_name", "")
        chance         = vert.get("chance", "")
        cand_action    = vert.get("cand_action", "")
        cac            = vert.get("cac", "")
    else:
        # Proxy: Date|SF_ID|IS_Person|MeetingID|Chance|Action|Categories
        pac              = ""
        proxy_action_str = ""
        if proxy_cells:
            proxy_action_str = proxy_cells[5] if len(proxy_cells) > 5 else ""
            pac              = proxy_cells[6] if len(proxy_cells) > 6 else ""
        proxy_action = proxy_action_str

        # Candidate: SF_ID|Name|MeetingID|Chance|Action|Categories
        candidate_name  = ""
        chance          = ""
        cand_action_str = ""
        cac             = ""
        if cand_cells:
            # Find name — must be a non-numeric, non-ID looking cell
            # Typical order: SF_ID | Name | MeetingID | Chance | Action | Categories
            candidate_name  = cand_cells[1] if len(cand_cells) > 1 else ""
            # If name looks like a meeting ID (all digits), shift right
            if candidate_name and _re.match(r"^[\d]{6,}$", candidate_name.strip()):
                candidate_name  = cand_cells[2] if len(cand_cells) > 2 else ""
                chance          = cand_cells[3] if len(cand_cells) > 3 else ""
                cand_action_str = cand_cells[4] if len(cand_cells) > 4 else ""
                cac             = cand_cells[5] if len(cand_cells) > 5 else ""
            else:
                chance          = cand_cells[3] if len(cand_cells) > 3 else ""
                cand_action_str = cand_cells[4] if len(cand_cells) > 4 else ""
                cac             = cand_cells[5] if len(cand_cells) > 5 else ""
        cand_action = cand_action_str

        if not chance and proxy_cells and len(proxy_cells) > 4:
            chance = proxy_cells[4]

    # Clean candidate name
    candidate_name = _re.sub(r"\s*\([A-Z]-\d+\)", "", candidate_name).strip()
    # Strip "N/A" placeholders
    if candidate_name.lower() in ("n/a", "na", "unknown", "candidate name", "name"):
        candidate_name = ""

    # Validate chance — must be numeric
    if chance:
        chance_clean = _re.sub(r"[^\d.]", "", chance.strip())
        try:
            v = float(chance_clean)
            chance = str(int(v)) if v == int(v) else chance_clean
            if v < 0 or v > 100:
                chance = ""
        except (ValueError, TypeError):
            chance = ""

    # ── Round Type ─────────────────────────────────────────────────────────
    round_type = ""
    for rtp in [
        r"\|\s*Round Type\s*\|\s*([^|\n]+)\|",
        r"round_type_detected[,:\s]+([^\n,]+)",
        r"Round Type[:\s]+([A-Za-z][\w_]+)",
    ]:
        rm = _re.search(rtp, text, _re.IGNORECASE)
        if rm:
            val = rm.group(1).strip().strip("*").strip()
            if val and not _re.match(r"^[\d./]+$", val):
                round_type = val
            if round_type:
                break

    # ── Scores ─────────────────────────────────────────────────────────────
    proxy_score = ""
    cand_score  = ""
    for field, is_proxy in [("Proxy Score", True), ("Candidate Score", False)]:
        for sp in [
            r"\|\s*" + _re.escape(field) + r"\s*\|\s*([\d.]+)\s*/\s*10\s*\|",
            _re.escape(field) + r"[^\n]*?([\d.]+)\s*/\s*10",
        ]:
            sm = _re.search(sp, text, _re.IGNORECASE)
            if sm:
                val = sm.group(1).strip() + "/10"
                if is_proxy:
                    proxy_score = val
                else:
                    cand_score  = val
                break

    # ── Duration & Total Questions ──────────────────────────────────────────
    duration = ""
    for dp in [
        r"\|\s*(?:Session\s+)?Duration\s*\|\s*([^|\n]+)\|",
        r"Duration[:\s]+([\d.]+\s*(?:min|minutes|hrs|hours)[^,\n]*)",
        r"Duration[:\s]+([^,\n]{1,30})",
    ]:
        dm = _re.search(dp, text, _re.IGNORECASE)
        if dm:
            duration = dm.group(1).strip().strip("*").strip().rstrip(",")
            if duration:
                break
    # Normalize duration format
    if duration:
        _dl = duration.strip().lower()
        _hm = _re.match(r"(\d+)\s*hours?\s*(\d+)\s*min", _dl)
        if _hm:
            duration = str(int(_hm.group(1))*60 + int(_hm.group(2))) + " minutes"
        elif _re.match(r"(\d+)\s*hours?\s*$", _dl):
            _h = _re.match(r"(\d+)", _dl)
            duration = str(int(_h.group(1))*60) + " minutes"
        elif _re.match(r"approx", _dl):
            _a = _re.search(r"(\d+)", _dl)
            if _a:
                duration = _a.group(1) + " minutes"
        elif _re.match(r"(\d+)\s*mins?\s*$", _dl):
            _mn = _re.match(r"(\d+)", _dl)
            duration = _mn.group(1) + " minutes"

    total_questions = ""
    for qp in [
        r"\|\s*Total Questions(?:\s+Asked)?\s*\|\s*([^|\n]+)\|",
        r"Total Questions[:\s]+(\d+)",
    ]:
        qm = _re.search(qp, text, _re.IGNORECASE)
        if qm:
            tq = qm.group(1).strip().strip("*").strip()
            num = _re.match(r"(\d+)", tq)
            total_questions = num.group(1) if num else tq
            if total_questions:
                break

    if not proxy_score:
        for sp in [r"Proxy [Ss]core[:\s]+([\d.]+)\s*/\s*10", r"proxy_score[:\s]+([\d.]+)\s*/\s*10"]:
            sm = _re.search(sp, text, _re.IGNORECASE)
            if sm:
                proxy_score = sm.group(1).strip() + "/10"
                break
    if not cand_score:
        for sp in [r"Candidate [Ss]core[:\s]+([\d.]+)\s*/\s*10", r"candidate_score[:\s]+([\d.]+)\s*/\s*10"]:
            sm = _re.search(sp, text, _re.IGNORECASE)
            if sm:
                cand_score = sm.group(1).strip() + "/10"
                break
    if not round_type:
        for rtp in [
            r"\|\s*Round Type\s*\|\s*([^|\n]+)\|",
            r"Round [Tt]ype[:\s]+([A-Za-z][\w_]+)",
            r"round_type[:\s]+([A-Za-z][\w_]+)",
        ]:
            rm = _re.search(rtp, text, _re.IGNORECASE)
            if rm:
                val = rm.group(1).strip().strip("*").strip()
                if val and not _re.match(r"^[\d./]+$", val):
                    round_type = val
                    break

    # ── Validate scores — reject action words accidentally parsed as scores ──
    _BAD_VALS = {"good","excellent","needs_improvement","needs improvement","critical","na","n/a","ok"}
    if proxy_score.lower().strip() in _BAD_VALS or not _re.match(r"^[\d.]+/10$", proxy_score):
        proxy_score = ""
    if cand_score.lower().strip() in _BAD_VALS or not _re.match(r"^[\d.]+/10$", cand_score):
        cand_score = ""

    # ── Full Final Verdict ──────────────────────────────────────────────────
    verdict = ""
    for vp in [
        r"##\s+Final Verdict\s*\n(.*?)(?=\n##|\Z)",
        r"##\s+FINAL[_\s]VERDICT\s*\n(.*?)(?=\n##|\Z)",
    ]:
        vm = _re.search(vp, text, _re.IGNORECASE | _re.DOTALL)
        if vm:
            v = vm.group(1).strip()
            v = _re.sub(r"\*\*([^*]*)\*\*", r"\1", v)
            v = _re.sub(r"#{1,4}\s+", "", v)
            v = _re.sub(r"\n{3,}", "\n\n", v)
            for stop in ["## Insufficient", "## Mistake", "### Predicted", "insufficient_context", "## END OF"]:
                idx2 = v.find(stop)
                if idx2 > 0:
                    v = v[:idx2].strip()
            verdict = v.strip()
            if verdict:
                break

    # ── Validate — reject if we parsed a header row as data ─────────────────
    _HEADER_WORDS = {
        "candidate name", "name", "chance of moving", "chance %", "chance",
        "action required", "meeting id", "salesforce", "interview id",
        "date", "categories", "is person", "interview-success person",
    }
    if candidate_name.strip().lower() in _HEADER_WORDS:
        log.warning(f"Parser matched header row as data (name={candidate_name!r}) — discarding")
        return None

    if not candidate_name and not chance:
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
        "duration":                        duration,
        "total_questions":                 total_questions,
    }


def parse_llm(llm_txt: str) -> dict:
    """
    BULLETPROOF master parser.

    Strategy:
      1. Strip header (everything before ====)
      2. Try JSON on body
      3. Try Flat TOON on body
      4. Try JSON on full text
      5. Try Flat TOON on full text
      6. Regex fallback: scan line by line for any key=value pattern
      7. Return default (empty) dict — never raises, never returns {}

    Always returns dict with all required keys.
    """
    if not llm_txt or not llm_txt.strip():
        log.warning("llm.txt is empty — returning defaults")
        return _default_result()

    # ── Strip report header ───────────────────────────────────────────────────
    body = llm_txt
    sep  = re.search(r'={4,}\s*\n', llm_txt)
    if sep:
        body = llm_txt[sep.end():]

    # ── Attempt order: Markdown Tables → JSON → FlatTOON ────────────────────
    # Markdown tables = new prompt v3.0 format (priority)
    # JSON = OpenAI old format
    # FlatTOON = Bedrock old format
    for attempt_text, fmt in [
        (llm_txt,  "markdown"),
        (body,     "markdown"),
        (body,     "json"),
        (body,     "flat_toon"),
        (llm_txt,  "json"),
        (llm_txt,  "flat_toon"),
    ]:
        try:
            if fmt == "markdown":
                result = _parse_markdown_tables(attempt_text)
            elif fmt == "json":
                result = _parse_json(attempt_text)
            else:
                result = _parse_flat_toon(attempt_text)

            if result and (result.get("candidate_name") or result.get("chance")):
                log.info(
                    f"  Parser={fmt} ✅ "
                    f"candidate='{result['candidate_name']}' "
                    f"chance='{result['chance']}' "
                    f"cand_action={result['candidate_action_required']} "
                    f"proxy_action={result['proxy_support_action_required']} "
                    f"cac='{result['candidate_action_categories'][:60]}' "
                    f"pac='{result['proxy_support_action_categories'][:60]}'"
                )
                return result
        except Exception as e:
            log.warning(f"  Parser={fmt} error: {e}")
            continue

    # ── Last resort: raw regex scan ───────────────────────────────────────────
    log.warning("All parsers failed — trying raw regex fallback")
    result = _default_result()

    for line in llm_txt.splitlines():
        line = line.strip()
        if not line or "," not in line:
            continue
        idx = line.find(",")
        key = line[:idx].strip().lower().replace(" ", "_")
        val = _strip_quotes(line[idx + 1:].strip())

        if not val:
            continue

        if "candidate_name" in key:
            result["candidate_name"] = val
        elif "candidate_detected" in key:
            result["candidate_name"] = result["candidate_name"] or val
        elif "chance_of_moving" in key or "probability_percent" in key:
            result["chance"] = val
        elif key == "candidate_action_required":
            result["candidate_action_required"] = _to_bool(val)
        elif "proxy_support_action_required" in key:
            result["proxy_support_action_required"] = _to_bool(val)
        elif "one_line_verdict" in key:
            result["verdict"] = val
        elif "round_type_detected" in key:
            result["round_type"] = val

    if result["candidate_name"] or result["chance"]:
        log.info(
            f"  Parser=regex_fallback ✅ "
            f"candidate='{result['candidate_name']}' "
            f"chance='{result['chance']}'"
        )
        return result

    log.warning(
        f"  All parsers failed — returning defaults. "
        f"First 300 chars:\n{llm_txt[:300]}"
    )
    return _default_result()


# ══════════════════════════════════════════════════════════════════════════════
# LLM SUPPORTING DATA EXTRACTORS
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#                    LLM-POWERED PARSER (GPT-4o-mini fallback)
# ══════════════════════════════════════════════════════════════════════════════
#
# When the regex parser fails or returns incomplete data (e.g., empty name,
# missing chance, no scores), we fall back to GPT-4o-mini for structured
# extraction. Results are cached in S3 keyed by llm.txt content hash so we
# never pay twice for the same file.
#
# Setup: requires OPENAI_API_KEY in env (already set for llm_processor_worker).
# Cost: ~$0.0001 per parse (4K tokens in, 1K tokens out at gpt-4o-mini pricing).

import hashlib
try:
    from openai import OpenAI as _OpenAI
    _OPENAI_IMPORT_OK = True
except ImportError as _e:
    _OPENAI_IMPORT_OK = False
    _OPENAI_IMPORT_ERR = str(_e)
    log.error(f"OpenAI library NOT installed: {_e}. Run: pip3 install openai --user --break-system-packages")

_openai_client = None
def _get_openai():
    """
    Get cached OpenAI client. Tries env var first, then AWS Secrets Manager.
    Logs why it fails on first call.
    """
    global _openai_client
    if not _OPENAI_IMPORT_OK:
        return None
    if _openai_client is not None:
        return _openai_client

    # Try env var first
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    source  = "env"

    # Fallback: AWS Secrets Manager (where llm_processor_worker stores it)
    if not api_key:
        try:
            sec = get_secret(API_SECRET_NAME)
            # Try common key names in the secret
            for key_name in ("OPENAI_API_KEY", "openai_api_key", "openai-api-key", "OPENAI_KEY"):
                if key_name in sec and sec[key_name]:
                    api_key = str(sec[key_name]).strip()
                    source  = f"secrets-manager:{key_name}"
                    break
        except Exception as e:
            log.error(f"Could not read OpenAI key from Secrets Manager: {e}")

    if not api_key:
        log.error("OPENAI_API_KEY not found in env OR Secrets Manager (secrets/api)")
        log.error("  Expected: secret \"secrets/api\" with key \"OPENAI_API_KEY\"")
        return None
    if not api_key.startswith("sk-"):
        log.error(f"OpenAI key looks invalid (starts with {api_key[:5]!r}, source={source})")
        return None
    try:
        _openai_client = _OpenAI(api_key=api_key)
        log.info(f"OpenAI client initialized (key {api_key[:8]}...{api_key[-4:]}, source={source})")
        return _openai_client
    except Exception as e:
        log.error(f"Failed to create OpenAI client: {e}")
        return None


# JSON schema for structured output — guarantees consistent shape
LLM_PARSER_SCHEMA = {
    "type": "object",
    "properties": {
        "candidate_name":                  {"type": "string"},
        "chance":                          {"type": "string"},
        "candidate_action_required":       {"type": "string"},
        "proxy_support_action_required":   {"type": "string"},
        "candidate_action_categories":     {"type": "string"},
        "proxy_support_action_categories": {"type": "string"},
        "candidate_score":                 {"type": "string"},
        "proxy_score":                     {"type": "string"},
        "round_type":                      {"type": "string"},
        "duration":                        {"type": "string"},
        "total_questions":                 {"type": "string"},
        "verdict":                         {"type": "string"},
        "questions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "question":  {"type": "string"},
                    "asked_at":  {"type": "string"},
                    "pasted_at": {"type": "string"},
                    "delta_sec": {"type": "string"},
                    "domain":    {"type": "string"},
                    "speed":     {"type": "string"},
                },
                "required": ["question", "asked_at", "pasted_at", "delta_sec", "domain", "speed"],
                "additionalProperties": False,
            },
        },
        "candidate_evidence": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "category":      {"type": "string"},
                    "vtt_timestamp": {"type": "string"},
                    "evidence":      {"type": "string"},
                },
                "required": ["category", "vtt_timestamp", "evidence"],
                "additionalProperties": False,
            },
        },
        "proxy_evidence": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "category":     {"type": "string"},
                    "doc_versions": {"type": "string"},
                    "evidence":     {"type": "string"},
                },
                "required": ["category", "doc_versions", "evidence"],
                "additionalProperties": False,
            },
        },
    },
    "required": [
        "candidate_name", "chance", "candidate_action_required",
        "proxy_support_action_required", "candidate_action_categories",
        "proxy_support_action_categories", "candidate_score", "proxy_score",
        "round_type", "duration", "total_questions", "verdict",
        "questions", "candidate_evidence", "proxy_evidence",
    ],
    "additionalProperties": False,
}


LLM_EXTRACTOR_PROMPT = """You are a data extraction tool. Extract structured data from this interview analysis report.

The report has TWO performance tables and supporting sections. Extract ALL fields exactly as written, even if formatting varies.

CRITICAL RULES:
1. candidate_name: from the Candidate Performance Table (NOT the Proxy table). Strip any (I-XXXX) suffix. If unknown/missing, return "".
2. chance: number 0-100 (no % sign). Return as string. If missing, return "".
3. candidate_score / proxy_score: format as "N/10" (e.g., "7/10", "5.5/10"). If missing, return "".
4. duration: normalize to "N minutes" (e.g., "1 hour 6 minutes" → "66 minutes", "Approximately 45" → "45 minutes"). If missing, return "".
5. round_type: just the type (Introduction_Call, Technical_Discussion, etc). NO numbers/scores. If missing, return "".
6. candidate_action_required / proxy_support_action_required: GOOD, EXCELLENT, NEEDS_IMPROVEMENT, or CRITICAL. If missing, return "".
7. categories: comma-separated list, no markdown. If missing, return "".
8. questions: extract from Response Speed Analysis section. delta_sec as string number (can be negative).
9. candidate_evidence: from Candidate Flag Details. vtt_timestamp = HH:MM:SS only (no "VTT" or "Timestamp" prefix in the value).
10. proxy_evidence: from Proxy Flag Details. doc_versions = "Version N" or "Versions N, M". If no version mentioned, return "".
11. evidence text: clean of markdown, no leading "evidence:" prefix, no surrounding quotes.
12. If a field cannot be found, return empty string "" (NOT null, NOT "N/A", NOT "Unknown").

Return ONLY valid JSON matching the schema. No markdown, no explanation."""


def _llm_cache_key(text: str) -> str:
    """Hash llm.txt content to use as cache key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _llm_cache_get(cache_key: str) -> dict:
    """Try to load cached parse result from S3."""
    try:
        s3_path = f"temp/llm-parser-cache/{cache_key}.json"
        resp = s3.get_object(Bucket=BUCKET, Key=s3_path)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception:
        return None


def _llm_cache_set(cache_key: str, data: dict):
    """Save parse result to S3 cache."""
    try:
        s3_path = f"temp/llm-parser-cache/{cache_key}.json"
        s3.put_object(
            Bucket=BUCKET, Key=s3_path,
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        log.warning(f"LLM cache write failed: {e}")


def parse_llm_with_gpt(text: str) -> dict:
    """
    Use GPT-4o-mini with structured output to extract all fields from llm.txt.
    Returns dict matching LLM_PARSER_SCHEMA, or None if API/import failure.
    Cached by content hash.
    """
    client = _get_openai()
    if client is None:
        log.warning("OpenAI client unavailable — skipping LLM parse")
        return None

    # Cache check
    cache_key = _llm_cache_key(text)
    cached = _llm_cache_get(cache_key)
    if cached is not None:
        log.info(f"  LLM parser cache HIT ({cache_key})")
        return cached

    # Truncate if needed (GPT-4o-mini has 128K context, but our llm.txt is ~3-6K chars)
    truncated = text[:50000]

    try:
        log.info(f"  LLM parser: calling GPT-4o-mini ({len(truncated)} chars)")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": LLM_EXTRACTOR_PROMPT},
                {"role": "user",   "content": truncated},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "interview_parse_result",
                    "strict": True,
                    "schema": LLM_PARSER_SCHEMA,
                },
            },
            temperature=0,
            max_tokens=4096,
        )
        raw = response.choices[0].message.content
        parsed = json.loads(raw)
        # Cache the result
        _llm_cache_set(cache_key, parsed)
        log.info(f"  LLM parser ✅ name={parsed.get('candidate_name','')!r} chance={parsed.get('chance','')!r}")
        return parsed
    except Exception as e:
        # Log full exception details — not just the message
        import traceback
        log.error(f"LLM parser FAILED: {type(e).__name__}: {e}")
        log.error(f"Traceback: {traceback.format_exc()[:500]}")
        return None


def parse_llm_smart(text: str) -> tuple:
    """
    GPT-4o-mini-only parser: always uses LLM for structured extraction.
    Cached by content hash so the same llm.txt is never parsed twice.
    Returns (parsed_dict, questions_list, candidate_evidence_list, proxy_evidence_list).
    """
    llm_result = parse_llm_with_gpt(text)

    if llm_result is None:
        log.error("  LLM parser FAILED \u2014 returning empty result")
        return _default_result(), [], [], []

    parsed = {
        "candidate_name":                  llm_result.get("candidate_name", ""),
        "chance":                          llm_result.get("chance", ""),
        "candidate_action_required":       llm_result.get("candidate_action_required", ""),
        "proxy_support_action_required":   llm_result.get("proxy_support_action_required", ""),
        "candidate_action_categories":     llm_result.get("candidate_action_categories", ""),
        "proxy_support_action_categories": llm_result.get("proxy_support_action_categories", ""),
        "candidate_score":                 llm_result.get("candidate_score", ""),
        "proxy_score":                     llm_result.get("proxy_score", ""),
        "verdict":                         llm_result.get("verdict", ""),
        "round_type":                      llm_result.get("round_type", ""),
        "duration":                        llm_result.get("duration", ""),
        "total_questions":                 llm_result.get("total_questions", ""),
    }
    questions = llm_result.get("questions", [])
    cand_ev   = llm_result.get("candidate_evidence", [])
    proxy_ev  = llm_result.get("proxy_evidence", [])

    return parsed, questions, cand_ev, proxy_ev

def _clean_cell(val: str) -> str:
    """Remove markdown bold (**text**), italic (*text*), and leading ~ from cell values."""
    import re as _re
    val = _re.sub(r'\*\*([^*]*)\*\*', r'\1', val)  # **bold** → text
    val = _re.sub(r'\*([^*]*)\*',   r'\1', val)  # *italic* → text
    val = val.lstrip('~').strip()                     # ~00:15 → 00:15
    return val.strip()


def extract_questions_from_llm(text: str) -> list:
    """
    Extract Response Speed Analysis table from LLM output.
    Returns list of dicts: question, asked_at, pasted_at, delta_sec, domain, speed
    """
    rows = []
    in_table = False
    header_found = False
    for line in text.split("\n"):
        stripped = line.strip()
        if re.search(r"Response Speed Analysis", stripped, re.IGNORECASE):
            in_table = True
            header_found = False
            continue
        if not in_table:
            continue
        if re.match(r"^\|[-| :]+\|$", stripped):
            header_found = True
            continue
        if header_found and stripped.startswith("|"):
            cells = [_clean_cell(c.strip()) for c in stripped.split("|") if c.strip()]
            if len(cells) >= 6:
                rows.append({
                    "question":   cells[1] if len(cells) > 1 else "",
                    "asked_at":   cells[2] if len(cells) > 2 else "",
                    "pasted_at":  cells[3] if len(cells) > 3 else "",
                    "delta_sec":  cells[4] if len(cells) > 4 else "",
                    "domain":     cells[5] if len(cells) > 5 else "",
                    "speed":      cells[6] if len(cells) > 6 else "",
                })
        elif in_table and header_found and stripped and stripped.startswith("#"):
            break

    if not rows:
        # Bullet format A (Title Case): - **Q&A Pair N**: Question: "...", Asked At: HH:MM:SS, ...
        # Bullet format B (snake_case):  - Q&A pair: "question text", asked_at: HH:MM:SS, pasted_at: ..., delta_seconds: N, domain_type: X, speed_rating: Y
        in_s = False
        for line in text.split("\n"):
            s2 = line.strip()
            if re.search(r"Response Speed Analysis", s2, re.IGNORECASE):
                in_s = True
                continue
            if not in_s:
                continue
            if s2.startswith("#") and "Speed" not in s2:
                break
            if not s2.startswith("-"):
                continue

            # Format A: Asked At / Pasted At (Title Case)
            a_m  = re.search(r'(?:Asked At|asked_at)[:\s]+([\d:]+)', s2, re.IGNORECASE)
            p_m  = re.search(r'(?:Pasted At|pasted_at)[:\s]+([\d:-]+)', s2, re.IGNORECASE)
            d_m  = re.search(r'(?:Delta Seconds?|delta_seconds?)[:\s]+(-?\d+)', s2, re.IGNORECASE)
            dt_m = re.search(r'(?:Domain Type|domain_type)[:\s]+([^,\n]+)', s2, re.IGNORECASE)
            sp_m = re.search(r'(?:Speed Rating|speed_rating)[:\s]+(\w+)', s2, re.IGNORECASE)

            # Question text: either format A "Question: text" or format B first quoted string
            q_m  = re.search(r'Question[:\s]+"?([^",\n]+)', s2, re.IGNORECASE)
            if not q_m:
                # Format B: Q&A pair: "question text", asked_at: ...
                q_m = re.search(r'Q&A pair[:\s]+"([^"]+)"', s2, re.IGNORECASE)
            if not q_m:
                # Fallback: first quoted string on the line (any length 5+ chars)
                q_m = re.search(r'"([^"]{5,})"', s2)

            if a_m or q_m:
                rows.append({
                    "question":  _clean_cell(q_m.group(1).strip()) if q_m else "",
                    "asked_at":  _clean_cell(a_m.group(1).strip()) if a_m else "",
                    "pasted_at": _clean_cell(p_m.group(1).strip()) if p_m else "",
                    "delta_sec": d_m.group(1).strip() if d_m else "",
                    "domain":    _clean_cell(dt_m.group(1).strip()) if dt_m else "",
                    "speed":     sp_m.group(1).strip() if sp_m else "",
                })
    return rows


def extract_proxy_evidence_from_llm(text: str) -> list:
    """
    Extract Proxy Flag Details table from LLM output.
    Returns list of dicts: category, doc_versions, evidence
    """
    rows = []
    in_table = False
    header_found = False
    for line in text.split("\n"):
        stripped = line.strip()
        if re.search(r"Proxy Flag Details", stripped, re.IGNORECASE):
            in_table = True
            header_found = False
            continue
        if not in_table:
            continue
        if re.match(r"^\|[-| :]+\|$", stripped):
            header_found = True
            continue
        if header_found and stripped.startswith("|"):
            cells = [_clean_cell(c.strip()) for c in stripped.split("|") if c.strip()]
            if len(cells) >= 3:
                rows.append({
                    "category":     cells[0],
                    "doc_versions": cells[1] if len(cells) > 1 else "",
                    "evidence":     cells[2] if len(cells) > 2 else "",
                })
        elif in_table and header_found and stripped and stripped.startswith("#"):
            break

    if not rows:
        # Format A: - **Category**: Version 5, Evidence text
        # Format B: - Category: X, triggered by versions N, evidence: text
        in_s = False
        for line in text.split("\n"):
            s2 = line.strip()
            if re.search(r"Proxy Flag Details", s2, re.IGNORECASE):
                in_s = True
                continue
            if not in_s:
                continue
            if s2.startswith("#") and "Proxy Flag" not in s2:
                break
            if not s2.startswith("-"):
                continue

            # Format A: - **Category Name**: Version 5, Evidence text
            # Format C: - **Category Name:** Version 5 ...   (colon INSIDE bold)
            m_a = re.match(r"-\s*\*\*([^*]+?):?\*\*:?\s*(.*)", s2)
            # Format B: - Category: Name, triggered by versions N, evidence: text
            m_b = re.match(r"-\s*Category:\s*([^,]+),\s*(.*)", s2, re.IGNORECASE)

            if m_a:
                cat  = m_a.group(1).rstrip(":").strip()
                rest = m_a.group(2).strip()
                # Try multiple version patterns:
                # "Version 5, Evidence text"
                # "Versions 4, 5, 6, Evidence text"  
                # "Version 2 triggered this flag due to ..."
                v_m = re.match(r"(Version[s]?\s*[\d,\s]+?)(?:[,]\s*|\s+(?=triggered|due|caused|because))(.*)", rest, re.IGNORECASE)
                if v_m:
                    rows.append({"category": _clean_cell(cat), "doc_versions": v_m.group(1).strip(), "evidence": _clean_cell(v_m.group(2).strip())})
                else:
                    rows.append({"category": _clean_cell(cat), "doc_versions": "", "evidence": _clean_cell(rest)})
            elif m_b:
                rest  = m_b.group(2).strip()
                ver_m = re.search(r"triggered by (?:versions?\s*)?([^,]+)", rest, re.IGNORECASE)
                ev_m  = re.search(r"evidence:\s*(.*)", rest, re.IGNORECASE)
                ver   = ver_m.group(1).strip() if ver_m else ""
                ev    = _clean_cell(ev_m.group(1).strip()) if ev_m else _clean_cell(rest)
                rows.append({"category": _clean_cell(m_b.group(1)), "doc_versions": ver, "evidence": ev})
    return rows


def extract_candidate_evidence_from_llm(text: str) -> list:
    """
    Extract Candidate Flag Details table from LLM output.
    Returns list of dicts: category, vtt_timestamp, evidence
    """
    rows = []
    in_table = False
    header_found = False
    for line in text.split("\n"):
        stripped = line.strip()
        if re.search(r"Candidate Flag Details", stripped, re.IGNORECASE):
            in_table = True
            header_found = False
            continue
        if not in_table:
            continue
        if re.match(r"^\|[-| :]+\|$", stripped):
            header_found = True
            continue
        if header_found and stripped.startswith("|"):
            cells = [_clean_cell(c.strip()) for c in stripped.split("|") if c.strip()]
            if len(cells) >= 3:
                cat   = cells[0]
                ts    = cells[1] if len(cells) > 1 else ""
                evid  = cells[2] if len(cells) > 2 else ""
                # Handle case where timestamp is embedded in evidence:
                # "VTT Timestamp: 00:08:20; notable pause..."
                if not ts and evid:
                    ts_m2 = re.match(r"VTT\s*[Tt]imestamp:?\s*([\d:]+)[;,]\s*(.*)", evid, re.IGNORECASE)
                    if ts_m2:
                        ts   = ts_m2.group(1).strip()
                        evid = ts_m2.group(2).strip()
                rows.append({"category": cat, "vtt_timestamp": ts, "evidence": evid})
        elif in_table and header_found and stripped and stripped.startswith("#"):
            break

    if not rows:
        # Format A: - **Category**: VTT timestamp 00:02:30, Evidence text
        # Format B: - Category: X, VTT timestamp: HH:MM:SS, evidence quote: "text"
        in_s = False
        for line in text.split("\n"):
            s2 = line.strip()
            if re.search(r"Candidate Flag Details", s2, re.IGNORECASE):
                in_s = True
                continue
            if not in_s:
                continue
            if s2.startswith("#") and "Candidate Flag" not in s2:
                break
            if not s2.startswith("-"):
                continue

            # Format A: - **Category Name**: VTT timestamp 00:02:30, evidence text
            # Format C: - **Category Name:** Timestamp 00:02:30, evidence text
            m_a = re.match(r"-\s*\*\*([^*]+?):?\*\*:?\s*(.*)", s2)
            # Format B: - Category: Name, VTT timestamp: HH:MM:SS, evidence quote: "text"
            m_b = re.match(r"-\s*Category:\s*([^,]+),\s*(.*)", s2, re.IGNORECASE)

            if m_a:
                cat  = m_a.group(1).rstrip(":").strip()
                rest = m_a.group(2).strip()
                # Try multiple timestamp patterns:
                # "00:02:15, evidence text"
                # "VTT timestamp 00:02:15, evidence text"
                # "Timestamp 00:02:15, evidence text"
                # "Timestamp: 00:02:15, evidence text"
                ts_m = re.match(r"(?:VTT\s*timestamp|Timestamp)?:?\s*([\d:]+)[,;]\s*(.*)", rest, re.IGNORECASE)
                if ts_m:
                    ev = ts_m.group(2).strip()
                    # Strip "evidence:" / "evidence quote:" prefix
                    ev = re.sub(r"^(evidence(?:\s*quote)?|quote):\s*\"?", "", ev, flags=re.IGNORECASE)
                    ev = ev.rstrip('"')
                    rows.append({"category": _clean_cell(cat), "vtt_timestamp": _clean_cell(ts_m.group(1).strip()), "evidence": _clean_cell(ev)})
                else:
                    rows.append({"category": _clean_cell(cat), "vtt_timestamp": "", "evidence": _clean_cell(rest)})
            elif m_b:
                rest  = m_b.group(2).strip()
                ts_m  = re.search(r"VTT timestamp:\s*([\d:]+)", rest, re.IGNORECASE)
                ev_m  = re.search(r"evidence(?:\s*quote)?:\s*\"?([^\"\n]+)", rest, re.IGNORECASE)
                ts    = _clean_cell(ts_m.group(1).strip()) if ts_m else ""
                ev    = _clean_cell(ev_m.group(1).strip()) if ev_m else _clean_cell(rest)
                rows.append({"category": _clean_cell(m_b.group(1)), "vtt_timestamp": ts, "evidence": ev})
    return rows

# ── Path helpers ──────────────────────────────────────────────────────────────
def _clean(s):
    return s.replace("_", " ").strip()

def extract_is_person(bp: str) -> str:
    """Extract IS person name from base_prefix (2nd segment after dept)."""
    parts = bp.split("/")
    if len(parts) >= 2 and parts[0] in DEPARTMENTS:
        return _clean(parts[1])
    return ""

def extract_year_month(bp: str) -> tuple:
    """Extract year and month name from base_prefix."""
    parts = bp.split("/")
    year = month_num = ""
    for p in parts:
        if p.isdigit() and len(p) == 4:
            year = p
        elif p.startswith("Month-"):
            try:
                month_num = int(p.replace("Month-", ""))
            except Exception:
                pass
    names = ["", "January", "February", "March", "April", "May", "June",
             "July", "August", "September", "October", "November", "December"]
    mn = (names[month_num]
          if isinstance(month_num, int) and 0 < month_num <= 12
          else "Unknown")
    return year, mn

# ── Google Drive cache + race-safe create ─────────────────────────────────────
_dc    = {}
_dc_lk = threading.Lock()
_cl    = {}
_cl_lk = threading.Lock()

def _lk(key):
    with _cl_lk:
        if key not in _cl:
            _cl[key] = threading.Lock()
        return _cl[key]

def _shared_drive(dsvc):
    k = "__sd__"
    with _dc_lk:
        if k in _dc:
            return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc:
                return _dc[k]
        for d in dsvc.drives().list(pageSize=20).execute().get("drives", []):
            if d["name"] == SHARED_DRIVE_NAME:
                with _dc_lk:
                    _dc[k] = d["id"]
                return d["id"]
        raise ValueError(f"Shared drive '{SHARED_DRIVE_NAME}' not found")

def _folder(dsvc, name, parent, drive_id):
    k = f"f:{parent}:{name}"
    with _dc_lk:
        if k in _dc:
            return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc:
                return _dc[k]
        resp = dsvc.files().list(
            q=(f"name='{name}' and "
               f"mimeType='application/vnd.google-apps.folder' "
               f"and '{parent}' in parents and trashed=false"),
            spaces="drive", fields="files(id,name)",
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            corpora="drive", driveId=drive_id,
        ).execute()
        files = resp.get("files", [])
        if files:
            fid = files[0]["id"]
            if len(files) > 1:
                log.warning(f"{len(files)} folders named '{name}' — using first")
        else:
            fid = dsvc.files().create(
                body={
                    "name": name,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent],
                },
                fields="id",
                supportsAllDrives=True,
            ).execute()["id"]
            log.info(f"✅ Created folder '{name}' → {fid}")
        with _dc_lk:
            _dc[k] = fid
        return fid

def _sheet(dsvc, ssvc, name, parent, drive_id):
    k = f"s:{parent}:{name}"
    with _dc_lk:
        if k in _dc:
            return _dc[k]
    with _lk(k):
        with _dc_lk:
            if k in _dc:
                return _dc[k]
        resp = dsvc.files().list(
            q=(f"name='{name}' and "
               f"mimeType='application/vnd.google-apps.spreadsheet' "
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
                body={
                    "name": name,
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "parents": [parent],
                },
                fields="id",
                supportsAllDrives=True,
            ).execute()["id"]
            log.info(f"✅ Created sheet '{name}': {sid}")
            _setup_tabs(ssvc, sid)
        with _dc_lk:
            _dc[k] = sid
        return sid

# ── Sheet tab setup ───────────────────────────────────────────────────────────
C_HDR = [
    "Date", "Salesforce Interview ID", "Candidate Name", "Meeting ID",
    "Chance of Moving to Next Round %", "Action Required",
    "Candidate Action Categories",
]
I_HDR = [
    "Date", "Salesforce Interview ID", "Interview-Success Person", "Meeting ID",
    "Chance of Moving to Next Round %", "Action Required",
    "Proxy Support Action Categories",
]
D_HDR = [
    "Date", "Salesforce Interview ID", "Candidate Name",
    "Interview-Success Person", "Meeting ID",
    "Chance of Moving to Next Round %",
    "Candidate Action Required", "Proxy Support Action Required",
    "Candidate Action Categories", "Proxy Support Action Categories",
    "Candidate Score", "Proxy Score", "Round Type",
    "Duration", "Total Questions",
    "Candidate Evidence", "Proxy Evidence", "Questions",
]

def _setup_tabs(ssvc, sid):
    meta   = ssvc.spreadsheets().get(spreadsheetId=sid).execute()
    exist  = meta.get("sheets", [])
    titles = [s["properties"]["title"] for s in exist]
    reqs   = []
    if exist and titles[0] != "Candidate":
        reqs.append({"updateSheetProperties": {
            "properties": {
                "sheetId": exist[0]["properties"]["sheetId"],
                "title": "Candidate",
            },
            "fields": "title",
        }})
    for t in ["Interview-Success", "Data"]:
        if t not in titles:
            reqs.append({"addSheet": {"properties": {"title": t}}})
    if reqs:
        ssvc.spreadsheets().batchUpdate(
            spreadsheetId=sid, body={"requests": reqs}
        ).execute()
    ssvc.spreadsheets().values().batchUpdate(
        spreadsheetId=sid,
        body={"valueInputOption": "RAW", "data": [
            {"range": "Candidate!A1",         "values": [C_HDR]},
            {"range": "Interview-Success!A1", "values": [I_HDR]},
            {"range": "Data!A1",              "values": [D_HDR]},
        ]},
    ).execute()
    log.info(f"Tabs set up for {sid}")

# ── Share sheet with configured emails ───────────────────────────────────────
_shared_sheets = set()   # cache: don't re-check same sheet twice per run
_shared_lk     = threading.Lock()

def _share_sheet_with_team(dsvc, sid: str):
    """
    Enforce EXACT access control on spreadsheet:
      1. Add SHARE_WITH_EMAILS as editors (if not already)
      2. REMOVE anyone else who has access (except service account owner)
    Only runs once per sheet per process run (cached).
    """
    with _shared_lk:
        if sid in _shared_sheets:
            return
        _shared_sheets.add(sid)

    allowed = {e.lower() for e in SHARE_WITH_EMAILS}

    # ── Step 1: Get current permissions ──────────────────────────────────────
    try:
        perms = dsvc.permissions().list(
            fileId=sid,
            fields="permissions(id,emailAddress,role,type)",
            supportsAllDrives=True,
        ).execute().get("permissions", [])
    except Exception as e:
        log.warning(f"Could not list permissions for {sid}: {e}")
        perms = []

    # ── Step 2: Remove anyone NOT in allowed list (skip owner/service acct) ──
    for perm in perms:
        email = perm.get("emailAddress", "").lower()
        perm_id = perm.get("id", "")
        role    = perm.get("role", "")
        ptype   = perm.get("type", "")

        # Never remove: owner, service account, anyone in allowed list
        if role == "owner":
            continue
        if not email:
            continue
        if email in allowed:
            continue
        if "gserviceaccount" in email:
            continue

        # Remove this person
        try:
            dsvc.permissions().delete(
                fileId=sid,
                permissionId=perm_id,
                supportsAllDrives=True,
            ).execute()
            log.info(f"🗑️  Removed {email} from {sid}")
        except Exception as e:
            log.warning(f"Could not remove {email} from {sid}: {e}")

    # ── Step 3: Add allowed emails if not already present ────────────────────
    existing_emails = {p.get("emailAddress", "").lower() for p in perms}
    for email in SHARE_WITH_EMAILS:
        if email.lower() in existing_emails:
            log.debug(f"Already has access: {email}")
            continue
        try:
            dsvc.permissions().create(
                fileId=sid,
                body={
                    "type":         "user",
                    "role":         "writer",
                    "emailAddress": email,
                },
                fields="id",
                supportsAllDrives=True,
                sendNotificationEmail=False,
            ).execute()
            log.info(f"✅ Shared {sid} with {email}")
        except Exception as e:
            log.warning(f"Share failed {email}: {e}")


# ── Sheet formatting: bold headers + column widths ────────────────────────────
_formatted_sheets = set()
_fmt_lk = threading.Lock()

def _format_sheet(ssvc, sid: str):
    """
    Apply formatting to the sheet ONCE per run:
    - Bold + freeze header row on all tabs
    - Auto-resize all columns
    Only runs once per spreadsheet per process run.
    """
    with _fmt_lk:
        if sid in _formatted_sheets:
            return
        _formatted_sheets.add(sid)

    try:
        meta   = ssvc.spreadsheets().get(spreadsheetId=sid).execute()
        sheets = meta.get("sheets", [])
        reqs   = []

        for sheet in sheets:
            gid   = sheet["properties"]["sheetId"]
            title = sheet["properties"]["title"]
            ncols = len(D_HDR) if title == "Data" else                     len(C_HDR) if title == "Candidate" else                     len(I_HDR) if title == "Interview-Success" else 10

            # Bold + background on header row
            reqs.append({"repeatCell": {
                "range": {"sheetId": gid, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": ncols},
                "cell": {"userEnteredFormat": {
                    "textFormat":        {"bold": True, "fontSize": 11},
                    "backgroundColor":   {"red": 0.23, "green": 0.47, "blue": 0.85},
                    "foregroundColor":   {"red": 1.0,  "green": 1.0,  "blue": 1.0},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment":   "MIDDLE",
                    "wrapStrategy":      "WRAP",
                }},
                "fields": "userEnteredFormat(textFormat,backgroundColor,foregroundColor,horizontalAlignment,verticalAlignment,wrapStrategy)",
            }})

            # Freeze header row
            reqs.append({"updateSheetProperties": {
                "properties": {"sheetId": gid,
                               "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }})

            # Set column widths
            col_widths = {
                "Data": [90, 130, 160, 130, 120, 80, 120, 120, 250, 250,
                         80, 80, 120, 80, 80, 130, 130, 130],
                "Candidate": [90, 130, 160, 120, 80, 120, 250],
                "Interview-Success": [90, 130, 130, 120, 80, 120, 250],
            }
            widths = col_widths.get(title, [150] * ncols)
            for ci, w in enumerate(widths[:ncols]):
                reqs.append({"updateDimensionProperties": {
                    "range": {"sheetId": gid, "dimension": "COLUMNS",
                              "startIndex": ci, "endIndex": ci + 1},
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize",
                }})

        if reqs:
            ssvc.spreadsheets().batchUpdate(
                spreadsheetId=sid, body={"requests": reqs}
            ).execute()
            log.info(f"✅ Sheet formatted: bold headers + column widths → {sid}")

    except Exception as e:
        log.warning(f"Format sheet error {sid}: {e}")


# ── Append row with retry ─────────────────────────────────────────────────────
def _append(ssvc, sid, tab, row, user_entered=False):
    """
    Append a row to a sheet tab.
    user_entered=True: use USER_ENTERED so =HYPERLINK() formulas are evaluated.
    user_entered=False: use RAW for plain data (faster, no formula parsing).
    Handles 429 rate limit and 400 "tab not found" by running _setup_tabs.
    """
    input_option = "USER_ENTERED" if user_entered else "RAW"
    for attempt in range(6):
        _token()
        try:
            ssvc.spreadsheets().values().append(
                spreadsheetId=sid,
                range=f"{tab}!A1",
                valueInputOption=input_option,
                insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            ).execute()
            return
        except HttpError as e:
            if e.resp.status == 429:
                wait = min((2 ** attempt) * 3, 30)
                log.warning(f"429 tab='{tab}' attempt {attempt+1}/6 sleep {wait}s")
                time.sleep(wait)
                if attempt == 5:
                    raise
            elif e.resp.status == 400 and "Unable to parse range" in str(e):
                # Tab doesn't exist yet — run setup and retry
                log.warning(f"400 tab='{tab}' not found — running _setup_tabs (attempt {attempt+1}/6)")
                try:
                    _setup_tabs(ssvc, sid)
                    time.sleep(2)
                except Exception as se:
                    log.warning(f"_setup_tabs error: {se}")
                if attempt == 5:
                    log.error(f"400 tab='{tab}' still not found after 6 attempts — giving up")
                    raise
                continue  # Retry the append
            else:
                log.error(f"Sheets error tab='{tab}': {e}")
                raise


# ══════════════════════════════════════════════════════════════════════════════
# EVIDENCE & QUESTION SHEET HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# Headers
CAND_EVIDENCE_HDR = ["Candidate Action Category", "VTT Timestamp", "Evidence"]
PRXY_EVIDENCE_HDR = ["Proxy Action Category",     "Doc Versions",  "Evidence"]
QUESTIONS_HDR     = ["Question", "Asked At", "Pasted At", "Delta Seconds", "Domain Type", "Speed Rating"]



def _validate_candidate_name(name: str) -> str:
    """
    Validate that a candidate name looks like a real person's name.
    Rejects narrative text, category labels, action words, placeholders.
    Returns cleaned name or empty string.
    """
    if not name or not str(name).strip():
        return ""
    n = str(name).strip()
    n = n.replace("**", "").replace("*", "").strip()
    nl = n.lower()
    REJECT_EXACT = {
        "unknown", "n/a", "na", "candidate name", "name", "high", "low",
        "critical", "good", "excellent", "needs_improvement", "moderate",
        "proxy", "candidate", "acceptable", "proxy coordination",
        "candidate coordination", "claude fine-tuning confusion",
    }
    if nl in REJECT_EXACT:
        return ""
    REJECT_KEYWORDS = ["confusion", "coordination", "transcript", "analysis",
                       "evidence", "rating", "version", "response", "polished",
                       "scripted", "interviewer", "candidate forced", "answers",
                       "noted at", "demonstrates", "exposed", "round"]
    for kw in REJECT_KEYWORDS:
        if kw in nl:
            return ""
    if len(n) > 50:
        return ""
    bad_patterns = [
        r'["“”]',
        r"[:;]",
        r"\.\.\.",
        r"[\(\[].{3,}[\)\]]",
        r"\d{2,}",
        r"[→—–]",
        r"[!?]",
        r"\.[A-Za-z]",
        r"[a-z]\.\s*$",
    ]
    for p in bad_patterns:
        if re.search(p, n):
            return ""
    words = n.split()
    if len(words) < 1 or len(words) > 5:
        return ""
    if not words[0][0].isalpha():
        return ""
    return n


def _safe_sheet_name(name: str) -> str:
    """Make a safe Google Sheets tab/file name (max 100 chars, no quotes/special chars)."""
    if not name or not str(name).strip():
        return "Unknown"
    # Remove characters that break Drive API queries or sheet names:
    # / \ [ ] * ? : ' " — and all control chars
    safe = re.sub(r"[/\\\[\]\*\?\:\'\"`]", " ", str(name))
    # Collapse multiple spaces
    safe = re.sub(r"\s+", " ", safe).strip()
    return safe[:100] if safe else "Unknown"


def get_or_create_evidence_sheet(dsvc, ssvc, drive_id, parent_folder_id,
                                  sheet_name: str, headers: list) -> str:
    """
    Get or create a spreadsheet with given name in parent_folder_id.
    Sets up header row if new. Returns spreadsheet_id.
    """
    safe_name = _safe_sheet_name(sheet_name)
    k = f"ev:{parent_folder_id}:{safe_name}"
    with _dc_lk:
        if k in _dc:
            return _dc[k]

    with _lk(k):
        with _dc_lk:
            if k in _dc:
                return _dc[k]

        # Check if sheet already exists
        resp = dsvc.files().list(
            q=(f"name='{safe_name}' and "
               f"mimeType='application/vnd.google-apps.spreadsheet' "
               f"and '{parent_folder_id}' in parents and trashed=false"),
            spaces="drive", fields="files(id,name)",
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            corpora="drive", driveId=drive_id,
        ).execute()
        files = resp.get("files", [])
        if files:
            sid = files[0]["id"]
        else:
            sid = dsvc.files().create(
                body={
                    "name":    safe_name,
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "parents": [parent_folder_id],
                },
                fields="id",
                supportsAllDrives=True,
            ).execute()["id"]
            # Set up header with retry
            for attempt in range(4):
                try:
                    time.sleep(0.3 * (attempt + 1))
                    ssvc.spreadsheets().values().update(
                        spreadsheetId=sid,
                        range="Sheet1!A1",
                        valueInputOption="RAW",
                        body={"values": [headers]},
                    ).execute()
                    break
                except HttpError as e:
                    if e.resp.status == 429 and attempt < 3:
                        time.sleep((2 ** attempt) * 5)
                    else:
                        raise
            log.info(f"✅ Created evidence sheet '{safe_name}' → {sid}")

        with _dc_lk:
            _dc[k] = sid
        return sid


def append_rows_to_sheet(ssvc, sid: str, rows: list, headers: list = None):
    """
    Write rows to Sheet1. CLEARS existing data first to prevent duplicates.
    Rewrites headers + all rows fresh every time.
    Retries on 429 rate limit with exponential backoff.
    """
    if not rows:
        return
    all_rows = []
    if headers:
        all_rows.append(headers)
    all_rows.extend(rows)

    for attempt in range(6):
        _token()
        try:
            # Step 1: Clear existing data
            ssvc.spreadsheets().values().clear(
                spreadsheetId=sid,
                range="Sheet1!A:Z",
            ).execute()
            time.sleep(0.5)  # Small pause between clear and write
            # Step 2: Write headers + data
            ssvc.spreadsheets().values().update(
                spreadsheetId=sid,
                range="Sheet1!A1",
                valueInputOption="RAW",
                body={"values": all_rows},
            ).execute()
            return
        except HttpError as e:
            if e.resp.status == 429:
                wait = min((2 ** attempt) * 3, 30)  # Cap at 30s
                log.warning(f"429 evidence sheet {sid} attempt {attempt+1}/6 sleep {wait}s")
                time.sleep(wait)
                if attempt == 5:
                    raise
            else:
                log.warning(f"append_rows_to_sheet error: {e}")
                raise
        except Exception as e:
            log.warning(f"append_rows_to_sheet error: {e}")
            raise


def make_hyperlink(url: str, label: str) -> str:
    """Create a Google Sheets HYPERLINK formula."""
    label_safe = label.replace('"', '\"'  )
    return f'=HYPERLINK("{url}","{label_safe}")'


def sheet_url(spreadsheet_id: str, gid: str = "0") -> str:
    """Build Google Sheets URL from spreadsheet_id."""
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

# ══════════════════════════════════════════════════════════════════════════════
# CORE PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def process(item: dict) -> str:
    mid = item["mid"]
    pfx = item["pfx"]
    log.info(f"[{mid}] ── Processing ──")

    # ── Step 1: Read llm-done.json ────────────────────────────────────────────
    raw = s3_read(f"{pfx}/llm-done.json")
    if not raw:
        return f"SKIP {mid} — no llm-done.json"
    try:
        llm_done = json.loads(raw)
    except Exception:
        return f"SKIP {mid} — llm-done.json parse error"

    # ── Step 2: Get base_prefix ───────────────────────────────────────────────
    bp = (
        llm_done.get("base_prefix", "")
        .replace(f"s3://{S3_BUCKET}/", "")
        .rstrip("/")
    )

    # Fallback: try done.json
    if not bp:
        done_raw = s3_read(f"{pfx}/done.json")
        if done_raw:
            try:
                d = json.loads(done_raw)
                bp = (
                    d.get("base_prefix", "")
                    or d.get("final_s3_prefix", "")
                ).replace(f"s3://{S3_BUCKET}/", "").rstrip("/")
            except Exception:
                pass

    log.info(f"[{mid}] base_prefix='{bp}'")

    # ── Step 3: Find llm.txt ──────────────────────────────────────────────────
    llm_txt = llm_key = ""

    # Try direct path first
    if bp:
        cand    = f"{bp}/llm/llm.txt"
        llm_txt = s3_read(cand)
        if llm_txt:
            llm_key = cand
            log.info(f"[{mid}] llm.txt found at: {llm_key}")

    # Scan S3 if not found
    if not llm_txt:
        log.info(f"[{mid}] Scanning S3 for llm.txt...")
        found_key = find_llm_key(mid)
        if found_key:
            llm_txt = s3_read(found_key)
            if llm_txt:
                llm_key = found_key
                bp      = find_base_prefix_from_llm_key(found_key, mid)
                log.info(f"[{mid}] llm.txt found via scan: {llm_key}")

    if not llm_txt:
        return f"SKIP {mid} — llm.txt not found anywhere"

    log.info(f"[{mid}] llm.txt: {len(llm_txt)} chars")

    # ── Step 4: Parse LLM output (GPT-4o-mini only) ───────────────────────────
    parsed, q_rows, ce_rows, pe_rows = parse_llm_smart(llm_txt)
    # parsed always has all keys — never None, never {}

    log.info(
        f"[{mid}] Parsed → "
        f"name='{parsed.get('candidate_name','')}' "
        f"chance='{parsed.get('chance','')}' "
        f"cand_action={parsed.get('candidate_action_required','')} "
        f"proxy_action={parsed.get('proxy_support_action_required','')} "
        f"cac='{parsed.get('candidate_action_categories','')[:50]}' "
        f"pac='{parsed.get('proxy_support_action_categories','')[:50]}'"
    )

    # ── Skip if LLM parser returned completely empty data (likely API failure) ─
    # This way we don't write garbage rows; the worker will retry on next pass.
    has_any_data = bool(
        parsed.get("candidate_name", "").strip() or
        parsed.get("chance", "").strip() or
        parsed.get("candidate_action_required", "").strip() or
        parsed.get("proxy_support_action_required", "").strip() or
        q_rows or ce_rows or pe_rows
    )
    if not has_any_data:
        # Track failures with retry counter — give up after 3 attempts
        fail_key = f"{pfx}/llm-parse-failed.json"
        try:
            resp = s3.get_object(Bucket=BUCKET, Key=fail_key)
            fail_data = json.loads(resp["Body"].read().decode("utf-8"))
            attempts = int(fail_data.get("attempts", 0)) + 1
        except Exception:
            attempts = 1
        s3_put_json(fail_key, {"attempts": attempts, "last_error": "GPT returned empty"})

        if attempts < 3:
            log.error(f"[{mid}] ❌ LLM parser EMPTY (attempt {attempts}/3) — will retry next run")
            log.error(f"[{mid}]    Check: 1) openai installed  2) OPENAI_API_KEY  3) API quota/status")
            return  # Don't write — will retry
        else:
            log.error(f"[{mid}] ❌ LLM parser failed 3 times — writing empty row to unblock pipeline")
            # Continue with empty data so we mark sheets-done and stop retrying

    # ── Step 5: Salesforce ────────────────────────────────────────────────────
    sf        = query_sf(mid)
    sf_id     = sf.get("sf_id", "")
    raw_name  = sf.get("name") or parsed.get("candidate_name", "") or ""
    # Validate name — reject garbage like "Over-polished paste language..." or "CRITICAL"
    cand_name = _validate_candidate_name(raw_name) or "Unknown"
    date_str  = (
        sf.get("date")
        or (datetime.now(timezone.utc) + IST).strftime("%Y-%m-%d")
    )

    # ── Step 6: Path metadata ─────────────────────────────────────────────────
    is_person        = extract_is_person(bp)
    year, month_name = extract_year_month(bp)
    if not year:
        now_ist    = datetime.now(timezone.utc) + IST
        year       = str(now_ist.year)
        month_name = now_ist.strftime("%B")

    log.info(
        f"[{mid}] is_person='{is_person}' "
        f"year={year} month={month_name}"
    )

    # ── Step 7: Google Sheets ─────────────────────────────────────────────────
    dsvc  = drive_svc_fn()
    ssvc  = sheets_svc_fn()
    did   = _shared_drive(dsvc)
    isf   = _folder(dsvc, GDRIVE_FOLDER, did,  did)
    yf    = _folder(dsvc, year,          isf,  did)
    mf    = _folder(dsvc, month_name,    yf,   did)
    sname = f"{month_name}_{year}"
    sid   = _sheet(dsvc, ssvc, sname, mf, did)

    # Share with team emails (once per sheet per run)
    _share_sheet_with_team(dsvc, sid)

    # Format sheet: bold headers + column widths (once per sheet per run)
    _format_sheet(ssvc, sid)

    # ── Step 8: Prepare values ────────────────────────────────────────────────
    cand_action  = str(parsed.get("candidate_action_required", "")).strip()
    proxy_action = str(parsed.get("proxy_support_action_required", "")).strip()
    chance       = parsed.get("chance", "")
    cac          = parsed.get("candidate_action_categories", "")
    pac          = parsed.get("proxy_support_action_categories", "")
    c_score      = parsed.get("candidate_score", "")
    p_score      = parsed.get("proxy_score", "")
    verdict      = parsed.get("verdict", "")
    round_type   = parsed.get("round_type", "")

    # Routing logic
    # Routing: always write both Candidate and IS tabs
    # (Action Required is now raw string like "GOOD"/"NEEDS_IMPROVEMENT"/true/false)
    write_c = True
    write_i = True
    routing_reason = "always_write_both"

    # ── Step 9: Extract evidence & questions from llm.txt ───────────────────
    llm_txt_raw = s3_read(llm_key) if llm_key else ""
    # q_rows, pe_rows, ce_rows already populated by parse_llm_smart() above
    log.info(f"[{mid}] Evidence: {len(ce_rows)} candidate, {len(pe_rows)} proxy, {len(q_rows)} questions")

    # ── Step 10: Create sub-folders for the month ─────────────────────────────
    cand_ev_folder = _folder(dsvc, "Candidate Evidence",       mf, did)
    prxy_ev_folder = _folder(dsvc, "Interview-Success Evidence", mf, did)
    quest_folder   = _folder(dsvc, "Questions",                mf, did)

    # ── Step 11: Create/update Candidate Evidence sheet ───────────────────────
    cand_ev_sid = ""
    if ce_rows:
        cand_ev_sid = get_or_create_evidence_sheet(
            dsvc, ssvc, did, cand_ev_folder,
            cand_name or mid, CAND_EVIDENCE_HDR,
        )
        append_rows_to_sheet(ssvc, cand_ev_sid, [
            [r["category"], r["vtt_timestamp"], r["evidence"]]
            for r in ce_rows
        ], headers=CAND_EVIDENCE_HDR)
        log.info(f"[{mid}] ✅ Candidate Evidence sheet written → {cand_ev_sid}")
        time.sleep(1)  # Pause between sheet writes to avoid 429

    # ── Step 12: Create/update Proxy Evidence sheet ───────────────────────────
    prxy_ev_sid = ""
    if pe_rows:
        prxy_ev_sid = get_or_create_evidence_sheet(
            dsvc, ssvc, did, prxy_ev_folder,
            is_person or mid, PRXY_EVIDENCE_HDR,
        )
        append_rows_to_sheet(ssvc, prxy_ev_sid, [
            [r["category"], r["doc_versions"], r["evidence"]]
            for r in pe_rows
        ], headers=PRXY_EVIDENCE_HDR)
        log.info(f"[{mid}] ✅ Proxy Evidence sheet written → {prxy_ev_sid}")
        time.sleep(1)

    # ── Step 13: Create/update Questions sheet ────────────────────────────────
    quest_sid = ""
    if q_rows:
        quest_sid = get_or_create_evidence_sheet(
            dsvc, ssvc, did, quest_folder,
            mid, QUESTIONS_HDR,
        )
        append_rows_to_sheet(ssvc, quest_sid, [
            [r["question"], r["asked_at"], r["pasted_at"],
             r["delta_sec"], r["domain"], r["speed"]]
            for r in q_rows
        ], headers=QUESTIONS_HDR)
        log.info(f"[{mid}] ✅ Questions sheet written → {quest_sid}")

    # ── Step 14: Build hyperlinks for Data tab ────────────────────────────────
    cand_ev_link  = make_hyperlink(sheet_url(cand_ev_sid), "Candidate Evidence") if cand_ev_sid else ""
    prxy_ev_link  = make_hyperlink(sheet_url(prxy_ev_sid), "Proxy Evidence")     if prxy_ev_sid else ""
    quest_link    = make_hyperlink(sheet_url(quest_sid),   "Questions")          if quest_sid   else ""

    # ── Step 15: Write Data tab (ALWAYS) ──────────────────────────────────────
    # Clean markdown from all parsed string fields
    def _clean_md(val):
        """Remove **bold**, *italic*, leading ~ from any cell value."""
        import re as _r
        if not isinstance(val, str): return val
        val = _r.sub(r'\*\*([^*]*)\*\*', r'\1', val)
        val = _r.sub(r'\*([^*]*)\*',   r'\1', val)
        return val.lstrip('~').strip()

    cand_action  = _clean_md(cand_action)
    proxy_action = _clean_md(proxy_action)
    cac          = _clean_md(cac)
    pac          = _clean_md(pac)
    c_score      = _clean_md(c_score)
    p_score      = _clean_md(p_score)
    round_type   = _clean_md(round_type)

    # Pull new fields from parsed
    duration        = _clean_md(parsed.get("duration", ""))
    total_questions = _clean_md(parsed.get("total_questions", ""))
    total_pastes    = parsed.get("total_pastes", "")

    # Data tab uses USER_ENTERED so =HYPERLINK() formulas are clickable
    # Build the data row — protect dates and scores from Sheets auto-conversion
    # by prefixing with a zero-width space when needed
    def _safe_text(val):
        """Prevent Google Sheets from converting strings like '6/10' or '2026-05-01' into dates."""
        if not val:
            return ""
        s = str(val).strip()
        # If looks like date/score that Sheets would auto-convert, prefix with apostrophe
        # Use ' (apostrophe) which Sheets treats as text marker (only with RAW)
        return s

    # Write A-O as RAW (text values preserved exactly)
    raw_row = [
        _safe_text(date_str), _safe_text(sf_id), _safe_text(cand_name),
        _safe_text(is_person), _safe_text(mid), _safe_text(chance),
        _safe_text(cand_action), _safe_text(proxy_action),
        _safe_text(cac), _safe_text(pac),
        _safe_text(c_score), _safe_text(p_score),
        _safe_text(round_type), _safe_text(duration), _safe_text(total_questions),
    ]
    # Write P-R hyperlinks as USER_ENTERED so =HYPERLINK formulas evaluate
    formula_row = [cand_ev_link, prxy_ev_link, quest_link]

    # Combine into one row, append once with USER_ENTERED
    # Use leading apostrophe on score values to force text mode
    full_row = list(raw_row) + list(formula_row)
    # Force text format on date and score columns by prepending apostrophe
    # Apostrophe prefix works in USER_ENTERED mode to force text - prevents
    # Google Sheets from converting "2026-05-07" → datetime, "6/10" → date
    full_row[0]  = "'" + str(date_str) if date_str else ""  # Date
    if c_score and "/" in c_score:
        full_row[10] = "'" + c_score  # Candidate Score
    if p_score and "/" in p_score:
        full_row[11] = "'" + p_score  # Proxy Score

    _append(ssvc, sid, "Data", full_row, user_entered=True)
    log.info(f"[{mid}] ✅ Data tab written")

    # ── Step 16: Candidate tab ────────────────────────────────────────────────
    if write_c:
        _append(ssvc, sid, "Candidate", [
            "'" + str(date_str) if date_str else "",
            sf_id, cand_name, mid, str(chance), cand_action, cac,
        ], user_entered=True)
        log.info(f"[{mid}] ✅ Candidate tab written")

    # ── Step 17: Interview-Success tab ────────────────────────────────────────
    if write_i:
        _append(ssvc, sid, "Interview-Success", [
            "'" + str(date_str) if date_str else "",
            sf_id, is_person, mid, str(chance), proxy_action, pac,
        ], user_entered=True)
        log.info(f"[{mid}] ✅ Interview-Success tab written")

    # ── Step 12: Write sheets-done.json ───────────────────────────────────────
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
        "tabs_written": {
            "Data":                    True,
            "Candidate":               write_c,
            "Interview-Success":       write_i,
            "Candidate Evidence":      bool(cand_ev_sid),
            "Proxy Evidence":          bool(prxy_ev_sid),
            "Questions":               bool(quest_sid),
        },
        "evidence_sheets": {
            "candidate_evidence_id":   cand_ev_sid,
            "proxy_evidence_id":       prxy_ev_sid,
            "questions_id":            quest_sid,
        },
        "routing_reason":   routing_reason,
        "sf_interview_id":  sf_id,
        "candidate_name":   cand_name,
        "is_person":        is_person,
        "parsed_data": {
            "chance":         chance,
            "cand_action":    cand_action,
            "proxy_action":   proxy_action,
            "cac":            cac,
            "pac":            pac,
            "candidate_score": c_score,
            "proxy_score":    p_score,
            "verdict":        verdict[:100] if verdict else "",
        },
    })
    log.info(f"[{mid}] ✅ sheets-done.json written")
    return f"OK {mid} → {sname} | {sid}"

# ══════════════════════════════════════════════════════════════════════════════
# LIVE POLLER + WORKERS + BACKFILL
# ══════════════════════════════════════════════════════════════════════════════

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
                        try:
                            live_q.put_nowait(item)
                            new += 1
                        except queue.Full:
                            log.warning(f"Queue full — dropping {item['mid']}")
            if new:
                log.info(f"Live poller: queued {new}")
        except Exception as e:
            log.error(f"Live poller error: {e}", exc_info=True)
        time.sleep(LIVE_POLL_INTERVAL)


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

            log.info(f"Backfill: {len(pending)} meetings")
            with ThreadPoolExecutor(
                max_workers=BACKFILL_WORKERS,
                thread_name_prefix="backfill",
            ) as ex:
                futures = {
                    ex.submit(process, item): item["mid"]
                    for item in pending
                }
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

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("sheets_writer_worker starting — BULLETPROOF VERSION")
    log.info(f"  Live workers:     {LIVE_WORKERS}")
    log.info(f"  Backfill workers: {BACKFILL_WORKERS}")
    log.info(f"  Departments:      {DEPARTMENTS}")
    log.info(f"  Shared drive:     {SHARED_DRIVE_NAME}")
    log.info("=" * 60)

    try:
        gcreds()
        log.info("Google credentials ✅")
    except Exception as e:
        log.error(f"Google creds failed: {e}")
        sys.exit(1)

    try:
        get_sf()
        log.info("Salesforce ✅")
    except Exception as e:
        log.error(f"Salesforce failed: {e}")
        sys.exit(1)

    try:
        sd_id = _shared_drive(drive_svc_fn())
        log.info(f"Shared drive '{SHARED_DRIVE_NAME}': {sd_id} ✅")
    except Exception as e:
        log.error(f"Shared drive not found: {e}")
        sys.exit(1)

    threading.Thread(
        target=live_poller, daemon=True, name="live-poller"
    ).start()
    threading.Thread(
        target=backfill_loop, daemon=True, name="backfill-loop"
    ).start()

    threads = []
    for i in range(LIVE_WORKERS):
        t = threading.Thread(
            target=live_worker, daemon=True, name=f"live-{i+1}"
        )
        t.start()
        threads.append(t)

    log.info(
        f"All workers started — "
        f"{LIVE_WORKERS} live + {BACKFILL_WORKERS} backfill"
    )

    while True:
        time.sleep(60)
        alive = sum(1 for t in threads if t.is_alive())
        log.info(f"Heartbeat — live={alive} queue={live_q.qsize()}")


if __name__ == "__main__":
    main()
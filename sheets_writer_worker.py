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
BACKFILL_WORKERS  = 10

# ONLY these 3 people should have access — all others will be removed
SHARE_WITH_EMAILS = [
    "naman.jain@techsarasolutions.com",
    "rajvi.patel@techsarasolutions.com",
    "sahil.patel@techsarasolutions.com",
    "techsphere@techsarasolutions.com",
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
    cand_action_str  = _get_field_flat_toon(body, "candidate_action_required")
    proxy_action_str = _get_field_flat_toon(body, "proxy_support_action_required")
    cand_action      = _to_bool(cand_action_str)
    proxy_action     = _to_bool(proxy_action_str)

    # ── action categories ─────────────────────────────────────────────────────
    cac = _extract_categories_flat_toon(body, "candidate_action_categories")
    pac = _extract_categories_flat_toon(body, "proxy_support_action_categories")

    # ── scores ────────────────────────────────────────────────────────────────
    # First score = candidate, second = proxy
    scores      = re.findall(r'^\s*score\s*,\s*(\d+)', body, re.MULTILINE)
    cand_score  = scores[0] if len(scores) > 0 else ""
    proxy_score = scores[1] if len(scores) > 1 else ""

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

    # Override action_required if ALL categories say "No Action Needed"
    NO_ACTION = ("no action needed", "no action required", "none", "n/a")
    if cac and all(any(p in c.lower() for p in NO_ACTION) for c in cac.split(";") if c.strip()):
        cand_action = False
    if pac and all(any(p in c.lower() for p in NO_ACTION) for c in pac.split(";") if c.strip()):
        proxy_action = False

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


def _default_result():
    """Empty result with all required keys."""
    return {
        "candidate_name":                  "",
        "chance":                          "",
        "candidate_action_required":       False,
        "proxy_support_action_required":   False,
        "candidate_action_categories":     "",
        "proxy_support_action_categories": "",
        "candidate_score":                 "",
        "proxy_score":                     "",
        "verdict":                         "",
        "round_type":                      "",
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

    # ── Attempt order: JSON body → FlatTOON body → JSON full → FlatTOON full ─
    for attempt_text, fmt in [
        (body,     "json"),
        (body,     "flat_toon"),
        (llm_txt,  "json"),
        (llm_txt,  "flat_toon"),
    ]:
        try:
            if fmt == "json":
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
    "Candidate Score", "Proxy Score", "Verdict", "Round Type",
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
            {"range": "'Candidate'!A1",         "values": [C_HDR]},
            {"range": "'Interview-Success'!A1", "values": [I_HDR]},
            {"range": "'Data'!A1",              "values": [D_HDR]},
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


# ── Append row with retry ─────────────────────────────────────────────────────
def _append(ssvc, sid, tab, row):
    for attempt in range(6):
        _token()
        try:
            ssvc.spreadsheets().values().append(
                spreadsheetId=sid,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            ).execute()
            return
        except HttpError as e:
            if e.resp.status == 429:
                wait = (2 ** attempt) * 5
                log.warning(
                    f"429 tab='{tab}' attempt {attempt+1}/6 sleep {wait}s"
                )
                time.sleep(wait)
                if attempt == 5:
                    raise
            else:
                log.error(f"Sheets error tab='{tab}': {e}")
                raise

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

    # ── Step 4: Parse LLM output ──────────────────────────────────────────────
    parsed = parse_llm(llm_txt)
    # parsed always has all keys — never None, never {}

    log.info(
        f"[{mid}] Parsed → "
        f"name='{parsed['candidate_name']}' "
        f"chance='{parsed['chance']}' "
        f"cand_action={parsed['candidate_action_required']} "
        f"proxy_action={parsed['proxy_support_action_required']} "
        f"cac='{parsed['candidate_action_categories'][:50]}' "
        f"pac='{parsed['proxy_support_action_categories'][:50]}'"
    )

    # ── Step 5: Salesforce ────────────────────────────────────────────────────
    sf        = query_sf(mid)
    sf_id     = sf.get("sf_id", "")
    cand_name = sf.get("name") or parsed.get("candidate_name", "") or "Unknown"
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

    # ── Step 8: Prepare values ────────────────────────────────────────────────
    yn           = lambda f: "Yes" if f else "No"
    cand_action  = parsed["candidate_action_required"]
    proxy_action = parsed["proxy_support_action_required"]
    chance       = parsed["chance"]
    cac          = parsed["candidate_action_categories"]
    pac          = parsed["proxy_support_action_categories"]
    c_score      = parsed["candidate_score"]
    p_score      = parsed["proxy_score"]
    verdict      = parsed["verdict"]
    round_type   = parsed["round_type"]

    # Routing logic
    write_c = cand_action  or (not cand_action and not proxy_action)
    write_i = proxy_action or (not cand_action and not proxy_action)

    routing_reason = (
        "both_true"      if (cand_action and proxy_action) else
        "candidate_only" if cand_action else
        "proxy_only"     if proxy_action else
        "both_false_write_both"
    )

    # ── Step 9: Write Data tab (ALWAYS) ───────────────────────────────────────
    _append(ssvc, sid, "Data", [
        date_str, sf_id, cand_name, is_person, mid, str(chance),
        yn(cand_action), yn(proxy_action),
        cac, pac, c_score, p_score, verdict, round_type,
    ])
    log.info(f"[{mid}] ✅ Data tab written")

    # ── Step 10: Candidate tab ────────────────────────────────────────────────
    if write_c:
        _append(ssvc, sid, "Candidate", [
            date_str, sf_id, cand_name, mid, str(chance), yn(cand_action), cac,
        ])
        log.info(f"[{mid}] ✅ Candidate tab written")

    # ── Step 11: Interview-Success tab ────────────────────────────────────────
    if write_i:
        _append(ssvc, sid, "Interview-Success", [
            date_str, sf_id, is_person, mid, str(chance), yn(proxy_action), pac,
        ])
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
            "Data":               True,
            "Candidate":          write_c,
            "Interview-Success":  write_i,
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
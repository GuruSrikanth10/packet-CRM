import os
from typing import Optional
import pandas as pd
from cachetools import TTLCache
from langchain_core.tools import tool
from sqlalchemy import create_engine
import pymysql
from src.utils.env import get_bool_env, get_required_env
from src.utils.resilience import retry_transient, db_breaker, es_breaker
from src.utils.logging_config import get_logger
import pybreaker

logger = get_logger(__name__)

_DB_CACHE = None
_LIVE_DB_ENGINE = None
# (target_col, {reason_code_str: [row_positions]}) built once from _DB_CACHE,
# or (None, {}) if the DB is empty/has no recognizable reason-code column.
# Avoids an O(n) astype(str)-and-compare scan of the whole table on every
# single lookup (2.4).
_RULE_INDEX_CACHE = None

# A plain functools.lru_cache here would cache for the process lifetime,
# including failure strings -- one transient MySQL blip would then poison
# that reason code until restart, and live rule edits would never take
# effect (1.1). TTLCache bounds both problems: entries expire on their own.
_RULE_CACHE_TTL_SECONDS = int(os.environ.get("RULE_CACHE_TTL_SECONDS", "600"))
_rule_cache: TTLCache = TTLCache(maxsize=256, ttl=_RULE_CACHE_TTL_SECONDS)

# Results that reflect a broken lookup (DB unreachable, misconfigured mock
# file) rather than a legitimate answer for this reason_code. These must
# never be cached -- caching them would keep serving the failure to every
# caller of this reason_code until the TTL expires.
_UNCACHEABLE_RESULT_PREFIXES = (
    "Failed to query live DB",
    "Mock database is empty",
    "Could not find a valid Reason Code column",
)


def _is_uncacheable_result(result: str) -> bool:
    return any(result.startswith(p) for p in _UNCACHEABLE_RESULT_PREFIXES)

def get_live_db_engine():
    global _LIVE_DB_ENGINE
    if _LIVE_DB_ENGINE is None:
        db_user = get_required_env("DB_USERNAME", "su01")
        db_pass = get_required_env("DB_PASSWORD", "su01")
        db_host = get_required_env("DB_HOST", "localhost")
        db_port = get_required_env("DB_PORT", "3306")
        db_name = get_required_env("DB_NAME", "uidmasterv1_1")
        _LIVE_DB_ENGINE = create_engine(
            f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}",
            pool_size=10, 
            max_overflow=20
        )
    return _LIVE_DB_ENGINE

def _load_mock_db():
    global _DB_CACHE
    if _DB_CACHE is not None:
        return _DB_CACHE

    # Use environment variable or default to src/db/mock_db.xlsx
    db_path = os.environ.get("MOCK_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "db", "mock_db.xlsx"))
    # Every branch below assigns to _DB_CACHE before returning -- previously
    # a missing/unreadable file fell through to an *uncached* empty
    # DataFrame, so os.path.exists() and a failed read were repeated on
    # every single call (2.4).
    if os.path.exists(db_path) and str(db_path).endswith((".xlsx", ".xls")):
        try:
            _DB_CACHE = pd.read_excel(db_path)
        except Exception as e:
            logger.error(f"Failed to load excel db: {e}")
            _DB_CACHE = pd.DataFrame()
    elif os.path.exists(db_path) and db_path.endswith(".csv"):
        try:
            _DB_CACHE = pd.read_csv(db_path)
        except Exception as e:
            logger.error(f"Failed to load csv db: {e}")
            _DB_CACHE = pd.DataFrame()
    else:
        _DB_CACHE = pd.DataFrame()
    return _DB_CACHE


def _build_rule_index():
    """Build (and cache) a reason_code -> row-position index over _DB_CACHE.

    Serializes to the same target column detection as before, but only
    scans the whole table once instead of on every lookup (2.4).
    """
    global _RULE_INDEX_CACHE
    if _RULE_INDEX_CACHE is not None:
        return _RULE_INDEX_CACHE

    db = _load_mock_db()
    if db is None or db.empty:
        _RULE_INDEX_CACHE = (None, {})
        return _RULE_INDEX_CACHE

    possible_cols = ['reasoncode', 'reason_code', 'errorcode', 'error_code', 'rejectioncode', 'rejection_code', 'code', 'rejectreasoncode', 'reject_reason_code']
    target_col = None
    for col in db.columns:
        clean_col = str(col).lower().replace(" ", "").replace("_", "")
        if clean_col in possible_cols:
            target_col = col
            break

    if not target_col:
        _RULE_INDEX_CACHE = (None, {})
        return _RULE_INDEX_CACHE

    index: dict = {}
    for pos, key in enumerate(db[target_col].astype(str)):
        index.setdefault(key, []).append(pos)

    _RULE_INDEX_CACHE = (target_col, index)
    return _RULE_INDEX_CACHE

@tool
def lookup_resident_database(uid: str, srn: str) -> str:
    """Lookup resident data from the excel database using UID or SRN to find out why a biometric packet failed."""
    db = _load_mock_db()
    if db is None or db.empty:
        return "Database is empty or could not be loaded from abd/abs."
        
    if 'srn' in db.columns and srn:
        matches = db[db['srn'].astype(str) == str(srn)]
        if not matches.empty:
            return matches.to_json(orient="records")
            
    if 'uid' in db.columns and uid:
        matches = db[db['uid'].astype(str) == str(uid)]
        if not matches.empty:
            return matches.to_json(orient="records")
            
    return "Resident not found in the database."

@tool
def lookup_error_code(error_code: str) -> str:
    """Lookup the meaning of an errorReasonCode (like RESIDENT_MAN_DEDUP_REJECT_TD)."""
    mock_errors = {
        "RESIDENT_MAN_DEDUP_REJECT_TD": "Manual deduplication rejected the packet due to a biometric anomaly.",
    }
    return mock_errors.get(error_code, "Unknown error code.")

# Registry mimicking agentic-fms
@tool
@db_breaker
@retry_transient
def lookup_rule_by_reason_code(reason_code: str) -> str:
    """Lookup the exact corresponding rule (including ruleId, payload, etc.) for a given reason code."""
    cached_result = _rule_cache.get(reason_code)
    if cached_result is not None:
        logger.info(f"[TOOL] Cache hit for rule lookup: {reason_code}")
        return cached_result

    result = _lookup_rule_by_reason_code_impl(reason_code)

    if not _is_uncacheable_result(result):
        _rule_cache[reason_code] = result

    return result


def _lookup_rule_by_reason_code_impl(reason_code: str) -> str:
    logger.info(f"[TOOL] lookup_rule_by_reason_code triggered for: {reason_code}")
    use_mock = get_bool_env("USE_MOCK_DB", True)
    if use_mock:
        db = _load_mock_db()
        if db is None or db.empty:
            logger.warning("[TOOL] Mock database is empty or could not be loaded!")
            return "Mock database is empty or could not be loaded."

        target_col, index = _build_rule_index()
        if not target_col:
            logger.warning("[TOOL] Could not find a valid Reason Code column in the DB!")
            return f"Could not find a valid Reason Code column in the DB. Available columns: {list(db.columns)}"

        positions = index.get(str(reason_code), [])
        if positions:
            matches = db.iloc[positions]
            logger.info(f"[TOOL] Found {len(matches)} matching rule(s) in DB for {reason_code}")
            return matches.to_json(orient="records")
        else:
            logger.info(f"[TOOL] No rules found in DB for {reason_code}")
            return f"Rule not found for reason code: {reason_code} in mock DB (Searched column: {target_col})."
    else:
        logger.info(f"[TOOL] Querying LIVE MySQL database for: {reason_code}")
        try:
            engine = get_live_db_engine()

            # Using pandas to query and format identically to the mock DB approach
            query = "SELECT * FROM rules WHERE reject_reason_code = %s"
            matches = pd.read_sql(query, engine, params=(reason_code,))

            if not matches.empty:
                logger.info(f"[TOOL] Found {len(matches)} matching rule(s) in Live DB for {reason_code}")
                return matches.to_json(orient="records")
            else:
                logger.info(f"[TOOL] No rules found in Live DB for {reason_code}")
                return f"Rule not found for reason code: {reason_code} in live DB."

        except Exception as e:
            logger.error(f"[TOOL] Live DB connection or query failed: {e}")
            return f"Failed to query live DB: {e}"


@tool
@es_breaker
@retry_transient
def fetch_elastic_logs(event_id: str) -> Optional[str]:
    """Fetch and reduce logs from Elastic using the 6-stage log reduction pipeline.

    Stages:
      1. Paginated ES fetch with source-filtering and catalog-driven must_not.
      2. Branch on ERROR presence (stuck path vs approve/reject path).
      3. Drain3 clustering with persisted state for stable template IDs.
      4. Evidence assembly guardrails (decision-vocabulary, rare templates, boundaries).

    Returns a compact, evidence-preserving string for LLM context injection,
    or None if logs could not be fetched/processed. Callers must check for
    None rather than pattern-matching an error-prefixed string -- there were
    previously two different failure-string prefixes ("Failed to query..."
    and "Failed to process logs...") and callers only ever checked for one
    of them, so the other silently flowed downstream as if it were log
    content (1.6).
    """
    log = logger.bind(event_id=event_id)
    log.info("[TOOL] fetch_elastic_logs triggered")

    try:
        from src.log_pipeline.pipeline import reduce_logs
        return reduce_logs(event_id)
    except pybreaker.CircuitBreakerError:
        log.error("[TOOL] ES Circuit breaker is OPEN. Failing fast.")
        return None
    except Exception as e:
        log.error(f"[TOOL] Log reduction pipeline failed: {e}")
        return None

@tool
def fetch_kubernetes_logs(pod_id: str) -> str:
    """Fetch logs from Kubernetes for a given pod or event identifier."""
    return f"[MOCK] Kubelet logs for {pod_id}: container killed due to OOMKilled state after biometric memory spike."

@tool
def queue_for_replay(id: str, idType: str, priority: int, operatorName: str, category: str, fromSedaStart: bool, notificationEmail: str, notificationMobile: str) -> str:
    """Queue a packet for replay through the OIS pipeline."""
    logger.info(f"[TOOL] queue_for_replay triggered for ID: {id}")

    payload = {
        "id": id,
        "idType": idType,
        "priority": priority,
        "operatorName": operatorName,
        "category": category,
        "fromSedaStart": fromSedaStart,
        "notificationEmail": notificationEmail,
        "notificationMobile": notificationMobile
    }
    
    enable_auto_replay = get_bool_env("ENABLE_AUTO_REPLAY", False)
    
    if enable_auto_replay:
        import requests
        base_url = os.environ.get("OIS_FEIGN_BASE_URL", "http://10.10.79.62:31261/ois/hold/v1")
        endpoint = f"{base_url}/api/v1/forceReplay"
        logger.info(f"[TOOL] Auto-replay enabled. Firing POST to {endpoint}")
        try:
            # Sent as a JSON body with an auth header rather than query
            # params -- query params land in server access logs, which would
            # leak notificationEmail/notificationMobile there (1.8).
            ois_api_key = os.environ.get("OIS_API_KEY")
            headers = {"X-API-Key": ois_api_key} if ois_api_key else {}
            response = requests.post(endpoint, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            return f"Successfully auto-replayed packet {id}: {response.text}"
        except Exception as e:
            logger.error(f"[TOOL] Failed to auto-replay {id}: {e}")
            return f"Failed to replay packet {id} directly: {e}"
    else:
        # Append to pending queue
        import json
        from filelock import FileLock
        from datetime import datetime
        
        base_dir = os.path.dirname(os.path.dirname(__file__))
        queue_file = os.path.join(base_dir, "db", "pending_replays.jsonl")
        os.makedirs(os.path.dirname(queue_file), exist_ok=True)
        lock_file = queue_file + ".lock"
        
        entry = {
            "timestamp": datetime.now().isoformat(),
            "payload": payload
        }
        
        try:
            with FileLock(lock_file, timeout=10):
                with open(queue_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry) + "\n")
            return f"Successfully queued packet {id} for human review before replay."
        except Exception as e:
            logger.error(f"[TOOL] Failed to queue replay for {id}: {e}")
            return f"Failed to queue packet {id}: {e}"

_TOOLS_MAP = {
    "lookup_resident_database": lookup_resident_database,
    "lookup_error_code": lookup_error_code,
    "lookup_rule_by_reason_code": lookup_rule_by_reason_code,
    "fetch_elastic_logs": fetch_elastic_logs,
    "fetch_kubernetes_logs": fetch_kubernetes_logs,
    "queue_for_replay": queue_for_replay
}

def get_tool_by_name(name: str):
    if name not in _TOOLS_MAP:
        raise ValueError(f"Tool {name} not found in registry")
    return _TOOLS_MAP[name]

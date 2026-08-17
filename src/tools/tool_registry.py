import json
import os
import threading
from typing import Optional
import pandas as pd
from cachetools import TTLCache
from langchain_core.tools import tool
from sqlalchemy import create_engine, text
# Imported for its side effect, not its name: SQLAlchemy resolves the
# "mysql+pymysql://" driver lazily, so without this a missing PyMySQL surfaces
# as a failed query on the first live lookup rather than at boot.
import pymysql  # noqa: F401
from src.utils.env import get_bool_env, get_required_env
from src.utils.resilience import retry_transient, db_breaker
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
# cachetools.TTLCache is explicitly NOT thread-safe -- its docs require
# external locking. This is read and written from up to
# MAX_CONCURRENT_INVESTIGATIONS agent threads, and TTL expiry mutates the
# internal linked list during __getitem__, so concurrent access can corrupt it
# or raise KeyError from inside the cache (F15).
_rule_cache_lock = threading.Lock()

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
            max_overflow=20,
            # Without pre-ping, a pooled connection that MySQL closed on its
            # wait_timeout is handed out anyway and the first query after an
            # idle period fails (F14). Recycle below that timeout as well.
            pool_pre_ping=True,
            pool_recycle=3600,
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
            logger.error("Failed to load Excel rules database", path=str(db_path), error=f"{type(e).__name__}: {e}")
            _DB_CACHE = pd.DataFrame()
    elif os.path.exists(db_path) and db_path.endswith(".csv"):
        try:
            _DB_CACHE = pd.read_csv(db_path)
        except Exception as e:
            logger.error("Failed to load CSV rules database", path=str(db_path), error=f"{type(e).__name__}: {e}")
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

# Maps the payload's terse enrolmentType onto the vocabulary the DB rule's
# `rule_data.statement.Condition.StringEquals.enrolmentType` actually uses.
# This mapping was previously inlined -- and inconsistently -- at four call
# sites: investigator_node defaulted an unknown type to None (no filtering)
# while all three runbook sites defaulted it to "UPDATE", silently filtering
# ENROLMENT rules out of an unrecognised packet type (F2).
_ENROLMENT_TYPE_ALIASES = {
    "U": "UPDATE",
    "UPDATE": "UPDATE",
    "E": "ENROLMENT",
    "ENROLMENT": "ENROLMENT",
    "ENROLLMENT": "ENROLMENT",
}


def normalize_enrolment_type(value: Optional[str]) -> Optional[str]:
    """Return the DB-side enrolment type, or None when it can't be determined.

    None means "do not filter" -- never "filter to the default". Guessing a
    type we don't have would drop the very rule the Investigator needs.
    """
    if not value:
        return None
    return _ENROLMENT_TYPE_ALIASES.get(str(value).strip().upper())


def _lookup_rule_json(reason_code: str) -> str:
    """Cached, breaker-protected raw lookup. Returns the impl's JSON string
    (or its human-readable failure message).

    This is the single protected path shared by the LLM-facing `@tool` and by
    the typed `lookup_rule_for` / `lookup_rule_text` helpers, so callers can't
    accidentally bypass the cache, the retry, or the circuit breaker.

    The cache read is deliberately OUTSIDE the breaker. With it inside, an open
    `db_breaker` raised CircuitBreakerError before the function body ran, so a
    reason code whose rule was already cached and unexpired still failed --
    and investigator_node substituted "Rule lookup failed for reason code X"
    into the prompt. Every packet got a degraded prompt during a 60-second
    MySQL blip, including the high-volume codes sitting valid in memory, which
    is exactly what the TTL cache was added to prevent (G11).
    """
    with _rule_cache_lock:
        cached_result = _rule_cache.get(reason_code)
    if cached_result is not None:
        logger.info("Rule lookup cache hit", reason_code=reason_code)
        return cached_result

    result = _lookup_rule_uncached(reason_code)

    if not _is_uncacheable_result(result):
        with _rule_cache_lock:
            _rule_cache[reason_code] = result

    return result


@db_breaker
@retry_transient
def _lookup_rule_uncached(reason_code: str) -> str:
    """The protected path: retried, breaker-guarded, and only reached on a
    cache miss.

    Runs outside `_rule_cache_lock` on purpose: a live-DB query can take
    seconds, and holding the lock across it would serialise every concurrent
    packet behind one query. A duplicate concurrent lookup is cheap; a
    serialised pipeline is not.
    """
    return _lookup_rule_by_reason_code_impl(reason_code)


# Registry mimicking agentic-fms
@tool
def lookup_rule_by_reason_code(reason_code: str) -> str:
    """Lookup the exact corresponding rule (including ruleId, payload, etc.) for a given reason code."""
    return _lookup_rule_json(reason_code)


def lookup_rule_for(reason_code: str,
                    enrolment_type: Optional[str] = None) -> Optional[list]:
    """Return the parsed rule rows for a reason code, filtered by enrolment type.

    Returns None when the lookup failed or matched nothing -- callers that
    need to distinguish "no rule" from "the DB is down" should check the
    breaker, not this return value.

    Exists because `lookup_rule_by_reason_code` is a `StructuredTool` under
    langchain-core 1.x: it is not callable, and it takes one argument. Three
    call sites invoked it directly with two, raising `TypeError` -- which in
    `runbook_lookup_node` was uncaught and sent every runbook-matching packet
    to the DLQ (F2). Fingerprinting also needs the *parsed* rows: hashing the
    raw `to_json` string folds DataFrame column order into the fingerprint, so
    a harmless re-export invalidated every runbook.
    """
    raw = _lookup_rule_json(reason_code)
    return _parse_and_filter_rules(raw, enrolment_type)


def lookup_rule_text(reason_code: str,
                     enrolment_type: Optional[str] = None) -> str:
    """Return the LLM-facing rule text for a reason code.

    Falls back to the raw lookup string when it isn't parseable JSON, because
    that string carries a meaningful message ("Rule not found for reason
    code: X") that the Investigator should see rather than an empty prompt.
    """
    raw = _lookup_rule_json(reason_code)
    filtered = _parse_and_filter_rules(raw, enrolment_type)
    if filtered is None:
        return raw
    return json.dumps(filtered)


def _parse_and_filter_rules(raw: str,
                            enrolment_type: Optional[str] = None) -> Optional[list]:
    """Parse the lookup's JSON-array string and filter it by enrolment type.

    A rule carrying no `enrolmentType` condition applies to every type and is
    always kept. If filtering would leave nothing, the unfiltered rows are
    returned instead -- a rule that matched the reason code is better evidence
    than no rule at all.
    """
    if not raw or not raw.lstrip().startswith("["):
        return None

    try:
        rules = json.loads(raw)
    except (ValueError, TypeError) as e:
        logger.warning("Rule lookup result was not parseable JSON", error=str(e))
        return None

    if not isinstance(rules, list) or not rules:
        return None

    target_type = normalize_enrolment_type(enrolment_type)
    if not target_type:
        return rules

    filtered = []
    for rule in rules:
        try:
            rule_data = json.loads(rule.get("rule_data", "{}"))
            condition = rule_data.get("statement", {}).get("Condition", {})
            rule_enrol_type = condition.get("StringEquals", {}).get("enrolmentType")
            if rule_enrol_type == target_type or not rule_enrol_type:
                filtered.append(rule)
        except Exception:
            # An unparseable rule_data is not grounds for hiding the rule.
            filtered.append(rule)

    return filtered or rules


def _lookup_rule_by_reason_code_impl(reason_code: str) -> str:
    logger.info("Rule lookup started", reason_code=reason_code)
    use_mock = get_bool_env("USE_MOCK_DB", True)
    if use_mock:
        db = _load_mock_db()
        if db is None or db.empty:
            logger.warning("Mock rules database is empty or could not be loaded", reason_code=reason_code)
            return "Mock database is empty or could not be loaded."

        target_col, index = _build_rule_index()
        if not target_col:
            logger.warning("No recognised reason-code column in the rules database", columns=[str(c) for c in db.columns])
            return f"Could not find a valid Reason Code column in the DB. Available columns: {list(db.columns)}"

        positions = index.get(str(reason_code), [])
        if positions:
            matches = db.iloc[positions]
            logger.info("Rules matched", reason_code=reason_code, source="mock", match_count=len(matches))
            return matches.to_json(orient="records")
        else:
            logger.info("No rules matched", reason_code=reason_code, source="mock", searched_column=str(target_col))
            return f"Rule not found for reason code: {reason_code} in mock DB (Searched column: {target_col})."
    else:
        logger.info("Rule lookup started", reason_code=reason_code, source="live")
        try:
            engine = get_live_db_engine()

            # Using pandas to query and format identically to the mock DB
            # approach. Must be text() with a :named bind: pandas wraps a raw
            # string in text() anyway, which uses SQLAlchemy's :name paramstyle
            # and expects a dict -- the old "%s" + tuple form is DBAPI
            # paramstyle and fails against a SQLAlchemy 2.x Engine (F14).
            query = text("SELECT * FROM rules WHERE reject_reason_code = :reason_code")
            matches = pd.read_sql(query, engine, params={"reason_code": reason_code})

            if not matches.empty:
                logger.info("Rules matched", reason_code=reason_code, source="live", match_count=len(matches))
                return matches.to_json(orient="records")
            else:
                logger.info("No rules matched", reason_code=reason_code, source="live")
                return f"Rule not found for reason code: {reason_code} in live DB."

        except Exception as e:
            logger.error("Live rules database query failed", reason_code=reason_code, error=f"{type(e).__name__}: {e}")
            return f"Failed to query live DB: {e}"


@retry_transient
def fetch_logs_for(event_id: str, extra_identifiers: tuple = ()) -> Optional[str]:
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

    Carries NO circuit breaker of its own. `@es_breaker` here guarded the
    whole source chain, so Kubernetes failures were counted against the
    Elasticsearch breaker -- twice, since k8s_breaker already wraps
    KubernetesLogSource.fetch -- and, once it opened, the healthy
    Elasticsearch fallback was refused too. The fallback chain was disabled
    by exactly the failure it exists to absorb (G10). Each source now owns
    its own breaker, where the failure is attributable.
    """
    log = logger.bind(event_id=event_id)
    log.info("Log fetch started", extra_identifiers=list(extra_identifiers or ()))

    try:
        from src.log_pipeline.pipeline import reduce_logs
        return reduce_logs(event_id, extra_identifiers=extra_identifiers)
    except pybreaker.CircuitBreakerError:
        log.error("Elasticsearch circuit breaker is open; failing fast", breaker="es_breaker")
        return None
    except Exception as e:
        log.error("Log reduction pipeline failed", error=f"{type(e).__name__}: {e}")
        return None


@tool
def fetch_elastic_logs(event_id: str) -> Optional[str]:
    """Fetch and reduce logs for an event_id using the log reduction pipeline.

    LLM-facing wrapper. Python callers should use `fetch_logs_for`, which also
    accepts the extra correlation identifiers (refId, srn) that the Kubernetes
    source matches on (F11) -- a tool signature can't carry those cleanly.
    """
    return fetch_logs_for(event_id)

@tool
def queue_for_replay(id: str, idType: str, priority: int, operatorName: str, category: str, fromSedaStart: bool) -> str:
    """Queue a packet for replay through the OIS pipeline."""
    logger.info("Replay queue requested", packet_id=id, id_type=idType)

    # notificationEmail / notificationMobile are deliberately NOT parameters.
    #
    # They were, which meant the LLM chose them -- and they are POSTed to the
    # OIS endpoint when ENABLE_AUTO_REPLAY=true. Nothing checked that the
    # address belonged to the packet in question, so a hallucinated or
    # log-scraped value would send a real notification about someone else's
    # enrolment (G20). Remediation 1.8 had already moved these out of query
    # params to keep them out of access logs, so the sensitivity was
    # understood; the provenance was not addressed.
    #
    # Omitted entirely rather than guessed: OIS can resolve the resident's
    # contact details from `id`, which is the authoritative source.
    payload = {
        "id": id,
        "idType": idType,
        "priority": priority,
        "operatorName": operatorName,
        "category": category,
        "fromSedaStart": fromSedaStart,
    }

    enable_auto_replay = get_bool_env("ENABLE_AUTO_REPLAY", False)
    
    if enable_auto_replay:
        import requests
        base_url = os.environ.get("OIS_FEIGN_BASE_URL", "http://10.10.79.62:31261/ois/hold/v1")
        endpoint = f"{base_url}/api/v1/forceReplay"
        logger.info("Auto-replay enabled; posting to the replay endpoint", packet_id=id, endpoint=endpoint)
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
            logger.error("Auto-replay failed", packet_id=id, error=f"{type(e).__name__}: {e}")
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
            logger.error("Failed to queue replay", packet_id=id, error=f"{type(e).__name__}: {e}")
            return f"Failed to queue packet {id}: {e}"

_TOOLS_MAP = {
    "lookup_resident_database": lookup_resident_database,
    "lookup_error_code": lookup_error_code,
    "lookup_rule_by_reason_code": lookup_rule_by_reason_code,
    "fetch_elastic_logs": fetch_elastic_logs,
    "queue_for_replay": queue_for_replay
}

def get_tool_by_name(name: str):
    if name not in _TOOLS_MAP:
        raise ValueError(f"Tool {name} not found in registry")
    return _TOOLS_MAP[name]

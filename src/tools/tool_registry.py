import os
import functools
import pandas as pd
from langchain_core.tools import tool
from sqlalchemy import create_engine
import pymysql
from src.utils.env import get_bool_env, get_required_env
from src.utils.resilience import retry_transient, db_breaker, es_breaker
import pybreaker

_DB_CACHE = None
_LIVE_DB_ENGINE = None

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
    if os.path.exists(db_path) and str(db_path).endswith((".xlsx", ".xls")):
        try:
            _DB_CACHE = pd.read_excel(db_path)
            return _DB_CACHE
        except Exception as e:
            print(f"Failed to load excel db: {e}")
            return None
    elif os.path.exists(db_path) and db_path.endswith(".csv"):
        try:
            _DB_CACHE = pd.read_csv(db_path)
            return _DB_CACHE
        except Exception as e:
            print(f"Failed to load csv db: {e}")
            return None
    return pd.DataFrame()

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
@functools.lru_cache(maxsize=128)
def lookup_rule_by_reason_code(reason_code: str) -> str:
    """Lookup the exact corresponding rule (including ruleId, payload, etc.) for a given reason code."""
    print(f"\n[TOOL] lookup_rule_by_reason_code triggered for: {reason_code}")
    use_mock = get_bool_env("USE_MOCK_DB", True)
    if use_mock:
        db = _load_mock_db()
        if db is None or db.empty:
            print("[TOOL] Mock database is empty or could not be loaded!")
            return "Mock database is empty or could not be loaded."
        
        # Find the column that might contain the reason code (case-insensitive and flexible)
        possible_cols = ['reasoncode', 'reason_code', 'errorcode', 'error_code', 'rejectioncode', 'rejection_code', 'code', 'rejectreasoncode', 'reject_reason_code']
        target_col = None
        for col in db.columns:
            clean_col = str(col).lower().replace(" ", "").replace("_", "")
            if clean_col in possible_cols:
                target_col = col
                break
                
        if target_col:
            matches = db[db[target_col].astype(str) == str(reason_code)]
            if not matches.empty:
                print(f"[TOOL] Found {len(matches)} matching rule(s) in DB for {reason_code}")
                return matches.to_json(orient="records")
            else:
                print(f"[TOOL] No rules found in DB for {reason_code}")
                return f"Rule not found for reason code: {reason_code} in mock DB (Searched column: {target_col})."
        
        print("[TOOL] Could not find a valid Reason Code column in the DB!")
        return f"Could not find a valid Reason Code column in the DB. Available columns: {list(db.columns)}"
    else:
        print(f"[TOOL] Querying LIVE MySQL database for: {reason_code}")
        try:
            engine = get_live_db_engine()
            
            # Using pandas to query and format identically to the mock DB approach
            query = "SELECT * FROM rules WHERE reject_reason_code = %s"
            matches = pd.read_sql(query, engine, params=(reason_code,))
            
            if not matches.empty:
                print(f"[TOOL] Found {len(matches)} matching rule(s) in Live DB for {reason_code}")
                return matches.to_json(orient="records")
            else:
                print(f"[TOOL] No rules found in Live DB for {reason_code}")
                return f"Rule not found for reason code: {reason_code} in live DB."
                
        except Exception as e:
            print(f"[TOOL] Live DB connection or query failed: {e}")
            return f"Failed to query live DB: {e}"


@tool
@es_breaker
@retry_transient
def fetch_elastic_logs(event_id: str) -> str:
    """Fetch and reduce logs from Elastic using the 6-stage log reduction pipeline.
    
    Stages:
      1. Paginated ES fetch with source-filtering and catalog-driven must_not.
      2. Branch on ERROR presence (stuck path vs approve/reject path).
      3. Drain3 clustering with persisted state for stable template IDs.
      4. Evidence assembly guardrails (decision-vocabulary, rare templates, boundaries).
    
    Returns a compact, evidence-preserving string for LLM context injection.
    """
    print(f"\n[TOOL] fetch_elastic_logs triggered for: {event_id}")
    
    try:
        from src.log_pipeline.pipeline import reduce_logs
        return reduce_logs(event_id)
    except pybreaker.CircuitBreakerError:
        print("[TOOL] ES Circuit breaker is OPEN. Failing fast.")
        return f"Failed to query Elastic: Circuit Breaker Open"
    except Exception as e:
        print(f"[TOOL] Log reduction pipeline failed: {e}")
        return f"Failed to process logs: {e}"

@tool
def fetch_kubernetes_logs(pod_id: str) -> str:
    """Fetch logs from Kubernetes for a given pod or event identifier."""
    return f"[MOCK] Kubelet logs for {pod_id}: container killed due to OOMKilled state after biometric memory spike."

@tool
def queue_for_replay(id: str, idType: str, priority: int, operatorName: str, category: str, fromSedaStart: bool, notificationEmail: str, notificationMobile: str) -> str:
    """Queue a packet for replay through the OIS pipeline."""
    print(f"\\n[TOOL] queue_for_replay triggered for ID: {id}")
    
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
        print(f"[TOOL] Auto-replay enabled. Firing POST to {endpoint}")
        try:
            response = requests.post(endpoint, params=payload, timeout=10)
            response.raise_for_status()
            return f"Successfully auto-replayed packet {id}: {response.text}"
        except Exception as e:
            print(f"[TOOL] Failed to auto-replay {id}: {e}")
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
                    f.write(json.dumps(entry) + "\\n")
            return f"Successfully queued packet {id} for human review before replay."
        except Exception as e:
            print(f"[TOOL] Failed to queue replay for {id}: {e}")
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

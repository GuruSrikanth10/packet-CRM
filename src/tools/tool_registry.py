import os
import pandas as pd
from langchain_core.tools import tool
from sqlalchemy import create_engine
import pymysql
from src.utils.env import get_bool_env, get_required_env

_DB_CACHE = None

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
def lookup_rule_by_reason_code(reason_code: str) -> str:
    """Lookup the exact corresponding rule (including ruleId, payload, etc.) for a given reason code."""
    print(f"\n[TOOL] 🔍 lookup_rule_by_reason_code triggered for: {reason_code}")
    use_mock = get_bool_env("USE_MOCK_DB", True)
    if use_mock:
        db = _load_mock_db()
        if db is None or db.empty:
            print("[TOOL] ❌ Mock database is empty or could not be loaded!")
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
                print(f"[TOOL] ✅ Found {len(matches)} matching rule(s) in DB for {reason_code}")
                return matches.to_json(orient="records")
            else:
                print(f"[TOOL] ⚠️ No rules found in DB for {reason_code}")
                return f"Rule not found for reason code: {reason_code} in mock DB (Searched column: {target_col})."
        
        print("[TOOL] ❌ Could not find a valid Reason Code column in the DB!")
        return f"Could not find a valid Reason Code column in the DB. Available columns: {list(db.columns)}"
    else:
        print(f"[TOOL] 🔍 Querying LIVE MySQL database for: {reason_code}")
        try:
            db_user = get_required_env("DB_USERNAME", "su01")
            db_pass = get_required_env("DB_PASSWORD", "su01")
            db_host = get_required_env("DB_HOST", "localhost")
            db_port = get_required_env("DB_PORT", "3306")
            db_name = get_required_env("DB_NAME", "uidmasterv1_1")
            
            engine = create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
            
            # Using pandas to query and format identically to the mock DB approach
            query = "SELECT * FROM rules WHERE reject_reason_code = %s"
            matches = pd.read_sql(query, engine, params=(reason_code,))
            
            if not matches.empty:
                print(f"[TOOL] ✅ Found {len(matches)} matching rule(s) in Live DB for {reason_code}")
                return matches.to_json(orient="records")
            else:
                print(f"[TOOL] ⚠️ No rules found in Live DB for {reason_code}")
                return f"Rule not found for reason code: {reason_code} in live DB."
                
        except Exception as e:
            print(f"[TOOL] ❌ Live DB connection or query failed: {e}")
            return f"Failed to query live DB: {e}"

@tool
def add_learning_rule(rule_text: str) -> str:
    """Appends a new permanent rule to the InvestigatorAgent's prompt file to correct mistakes."""
    target_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "InvestigatorAgent.md")
    
    # Ensure directory and file exist
    os.makedirs(os.path.dirname(target_file), exist_ok=True)
    
    try:
        with open(target_file, "a", encoding="utf-8") as f:
            f.write(f"\n- CRITICAL RULE: {rule_text}\n")
        return f"Successfully added rule to InvestigatorAgent: {rule_text}"
    except Exception as e:
        return f"Failed to add rule: {e}"

@tool
def fetch_elastic_logs(event_id: str) -> str:
    """Fetch logs from Elastic for a given event ID using pagination to capture the full trace."""
    print(f"\n[TOOL] 🔍 fetch_elastic_logs triggered for: {event_id}")
    
    es_host = os.environ.get("ES_HOST")
    es_user = os.environ.get("ES_USERNAME")
    es_pass = os.environ.get("ES_PASSWORD")
    index_pattern = os.environ.get("ES_INDEX_PATTERN", "logs-*")
    
    if not es_host:
        print("[TOOL] ❌ ES_HOST not set. Falling back to mock response.")
        return f"[MOCK] Elastic logs for {event_id}: ERROR - connection timeout. Stacktrace missing."
        
    try:
        from elasticsearch import Elasticsearch
        
        auth_args = {}
        if es_user and es_pass:
            auth_args["basic_auth"] = (es_user, es_pass)
            
        es_client = Elasticsearch(
            es_host,
            verify_certs=False,
            **auth_args
        )
        
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": f'"{event_id}"'
                            }
                        }
                    ],
                    "filter": [
                        {
                            "term": {
                                "application_name.keyword": "enu-biometric"
                            }
                        }
                    ]
                }
            }
        }

        sort_criteria = [
            {"@timestamp": {"order": "asc"}},
            {"_id": {"order": "asc"}}
        ]

        llm_context = []
        llm_context.append(f"--- Log Trace for ID: {event_id} ---")
        
        search_after_values = None
        page_size = 500
        total_fetched = 0

        while True:
            search_kwargs = {
                "index": index_pattern,
                "size": page_size,
                "sort": sort_criteria,
                "query": query_body["query"]
            }
            
            if search_after_values:
                search_kwargs["search_after"] = search_after_values

            response = es_client.search(**search_kwargs)
            hits = response.get("hits", {}).get("hits", [])
            
            if not hits:
                break
                
            for hit in hits:
                source = hit["_source"]
                timestamp = source.get("@timestamp", "UNKNOWN_TIME")
                app_name = source.get("application_name", 
                           source.get("kubernetes", {}).get("container", {}).get("name", 
                           source.get("HOSTNAME", "unknown-service")))
                level = source.get("level", "INFO")
                log_msg = source.get("message", source.get("msg", str(source)))
                
                llm_context.append(f"[{timestamp}] [{app_name}] [{level}] {log_msg}")
                search_after_values = hit["sort"]
                
            total_fetched += len(hits)
            
        print(f"[TOOL] ✅ Successfully fetched {total_fetched} logs from Elastic!")
        
        if total_fetched == 0:
            return f"No logs found for ID: {event_id}"

        llm_context.append(f"--- End of Trace ({total_fetched} logs total) ---")
        return "\n".join(llm_context)
        
    except Exception as e:
        print(f"[TOOL] ❌ Failed to fetch Elastic logs: {e}")
        return f"Failed to query Elastic: {e}"

@tool
def fetch_kubernetes_logs(pod_id: str) -> str:
    """Fetch logs from Kubernetes for a given pod or event identifier."""
    return f"[MOCK] Kubelet logs for {pod_id}: container killed due to OOMKilled state after biometric memory spike."

_TOOLS_MAP = {
    "lookup_resident_database": lookup_resident_database,
    "lookup_error_code": lookup_error_code,
    "lookup_rule_by_reason_code": lookup_rule_by_reason_code,
    "add_learning_rule": add_learning_rule,
    "fetch_elastic_logs": fetch_elastic_logs,
    "fetch_kubernetes_logs": fetch_kubernetes_logs
}

def get_tool_by_name(name: str):
    if name not in _TOOLS_MAP:
        raise ValueError(f"Tool {name} not found in registry")
    return _TOOLS_MAP[name]

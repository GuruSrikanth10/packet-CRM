import os
import pandas as pd
from langchain_core.tools import tool
from utils.env import get_bool_env

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
    use_mock = get_bool_env("USE_MOCK_DB", True)
    if use_mock:
        db = _load_mock_db()
        if db is None or db.empty:
            return "Mock database is empty or could not be loaded."
        
        if 'reasonCode' in db.columns:
            matches = db[db['reasonCode'].astype(str) == str(reason_code)]
            if not matches.empty:
                return matches.to_json(orient="records")
        return f"Rule not found for reason code: {reason_code} in mock DB."
    else:
        # Placeholder for actual DB lookup logic
        return f"[Actual DB Lookup Placeholder] Would lookup rule for {reason_code} in real DB."

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

_TOOLS_MAP = {
    "lookup_resident_database": lookup_resident_database,
    "lookup_error_code": lookup_error_code,
    "lookup_rule_by_reason_code": lookup_rule_by_reason_code,
    "add_learning_rule": add_learning_rule
}

def get_tool_by_name(name: str):
    if name not in _TOOLS_MAP:
        raise ValueError(f"Tool {name} not found in registry")
    return _TOOLS_MAP[name]

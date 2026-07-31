import os
import pandas as pd
from langchain_core.tools import tool

# Global cache for the database to avoid reading from disk on every query
_DB_CACHE = None

def _load_mock_db():
    global _DB_CACHE
    if _DB_CACHE is not None:
        return _DB_CACHE
        
    db_path = os.path.join(os.getcwd(), "abd", "abs")
    if os.path.exists(db_path) and db_path.endswith((".xlsx", ".xls")):
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
            
    # Return an empty dataframe if not found so it doesn't crash
    return pd.DataFrame()

@tool
def lookup_resident_database(uid: str, srn: str) -> str:
    """Lookup resident data from the excel database using UID or SRN to find out why a biometric packet failed."""
    db = _load_mock_db()
    if db is None or db.empty:
        return "Database is empty or could not be loaded from abd/abs."
        
    # Attempt to query the database. Assumes columns 'uid' or 'srn' exist.
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
    # Mock implementation for error codes
    mock_errors = {
        "RESIDENT_MAN_DEDUP_REJECT_TD": "Manual deduplication rejected the packet due to a biometric anomaly.",
    }
    return mock_errors.get(error_code, "Unknown error code.")

def get_agent_tools():
    """Return the list of tools available to the LangGraph agent."""
    return [lookup_resident_database, lookup_error_code]

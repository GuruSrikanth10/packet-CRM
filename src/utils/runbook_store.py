import os
import json
import re
import hashlib
from typing import Optional
from cachetools import TTLCache
from pathlib import Path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

RUNBOOK_ROOT = Path(__file__).resolve().parent.parent.parent / "src" / "runbooks"
RUNBOOK_FINAL_DIR = RUNBOOK_ROOT / "final"
RUNBOOK_DRAFT_DIR = RUNBOOK_ROOT / "draft"

# Ensure directories exist
os.makedirs(RUNBOOK_FINAL_DIR, exist_ok=True)
os.makedirs(RUNBOOK_DRAFT_DIR, exist_ok=True)

# Validation pattern for reason_code (0.11, 1.17 convention)
REASON_CODE_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")

# Module-level TTL cache for runbook loads
CACHE_TTL = int(os.environ.get("RUNBOOK_CACHE_TTL_SECONDS", "600"))
_runbook_cache = TTLCache(maxsize=1024, ttl=CACHE_TTL)

def generate_rule_fingerprint(rule_dict: dict) -> str:
    """Generate a stable SHA256 fingerprint from a rule dict."""
    canonical_json = json.dumps(rule_dict, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

def _resolve_runbook_path(reason_code: str, enrolment_type: str, directory: Path) -> Path:
    """Resolve and validate the runbook file path safely."""
    if not REASON_CODE_PATTERN.match(reason_code):
        raise ValueError(f"Invalid reason_code format: {reason_code}")
    
    # Enrolment type defaults to ANY if missing
    etype = str(enrolment_type).strip().upper() if enrolment_type else "ANY"
    
    filename = f"{reason_code}__{etype}.json"
    target_path = (directory / filename).resolve()
    
    # Path traversal guard
    if not str(target_path).startswith(str(directory.resolve())):
        raise ValueError(f"Resolved path escapes runbook directory: {target_path}")
        
    return target_path

def get_runbook(reason_code: str, enrolment_type: str) -> Optional[dict]:
    """
    Fetch a final runbook for the given reason code and enrolment type.
    Falls back to ANY enrolment_type if exact match fails.
    Uses TTLCache. Returns None on miss.
    """
    if not reason_code:
        logger.info("Runbook miss", miss_reason="no_reason_code")
        return None
        
    if not REASON_CODE_PATTERN.match(reason_code):
        logger.info("Runbook miss", reason_code=reason_code, enrolment_type=enrolment_type, miss_reason="invalid_key")
        return None

    # Determine types to try (exact match, then ANY fallback)
    etype = str(enrolment_type).strip().upper() if enrolment_type else "ANY"
    candidates = [(reason_code, etype)]
    if etype != "ANY":
        candidates.append((reason_code, "ANY"))
        
    for r_code, e_type in candidates:
        cache_key = f"{r_code}__{e_type}"
        if cache_key in _runbook_cache:
            return _runbook_cache[cache_key]
            
        try:
            target_path = _resolve_runbook_path(r_code, e_type, RUNBOOK_FINAL_DIR)
        except ValueError as e:
            logger.info("Runbook miss", reason_code=r_code, enrolment_type=e_type, miss_reason="invalid_key")
            continue
            
        if not target_path.exists():
            continue
            
        try:
            with open(target_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            # Basic validation
            if not isinstance(data, dict):
                raise ValueError("Runbook is not a JSON object")
            if data.get("schema_version") not in ("1.0", "1.1"):
                raise ValueError(f"Unsupported schema version: {data.get('schema_version')}")
            if data.get("status") != "final":
                raise ValueError(f"Status is not final: {data.get('status')}")
            
            resolution = data.get("resolution", {})
            required_keys = {"rejection_description", "synthesis", "action", "resident_action"}
            if not required_keys.issubset(resolution.keys()):
                raise ValueError(f"Missing resolution keys: {required_keys - set(resolution.keys())}")
                
            _runbook_cache[cache_key] = data
            return data
            
        except Exception as e:
            logger.error(f"Malformed runbook", path=str(target_path), error=str(e))
            logger.info("Runbook miss", reason_code=r_code, enrolment_type=e_type, miss_reason="malformed")
            
    logger.info("Runbook miss", reason_code=reason_code, enrolment_type=enrolment_type, miss_reason="not_found")
    return None

def write_draft_runbook(reason_code: str, enrolment_type: str, data: dict):
    """Write a draft runbook to disk. Cannot write to final."""
    target_path = _resolve_runbook_path(reason_code, enrolment_type, RUNBOOK_DRAFT_DIR)
    
    # Use atomic write via temp file
    tmp_path = target_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, target_path)

def list_draft_runbooks() -> list[Path]:
    """Return all draft runbook paths."""
    if not RUNBOOK_DRAFT_DIR.exists():
        return []
    return list(RUNBOOK_DRAFT_DIR.glob("*.json"))

def load_draft_runbook(path: Path) -> dict:
    """Load a draft runbook."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def promote_draft_to_final(draft_path: Path, data: dict):
    """Save to final/ and remove the draft."""
    reason_code = data["reason_code"]
    enrolment_type = data["enrolment_type"]
    
    final_path = _resolve_runbook_path(reason_code, enrolment_type, RUNBOOK_FINAL_DIR)
    tmp_path = final_path.with_suffix(".json.tmp")
    
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, final_path)
    
    # Invalidate cache
    cache_key = f"{reason_code}__{enrolment_type}"
    _runbook_cache.pop(cache_key, None)
    
    # Remove draft
    os.remove(draft_path)

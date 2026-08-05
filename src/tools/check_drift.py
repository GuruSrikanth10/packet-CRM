import os
import csv
import sys
import logging

logger = logging.getLogger(__name__)

# A dummy representation of the expected "DB Schema" policies/columns
KNOWN_DB_COLUMNS = {
    "rule_data",
    "reject_reason_code",
    "rule_description",
    "is_active"
}

def check_drift():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    rules_file = os.path.join(base_dir, "rules.csv")
    
    if not os.path.exists(rules_file):
        logger.error(f"Cannot find rules file at {rules_file}")
        sys.exit(1)
        
    print(f"Checking {rules_file} against expected schema...")
    
    with open(rules_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            logger.error("Rules file is empty!")
            sys.exit(1)
            
    # Normalize headers
    headers = [h.strip().lower() for h in headers if h.strip()]
    
    unknown_columns = [h for h in headers if h not in KNOWN_DB_COLUMNS]
    missing_columns = [k for k in KNOWN_DB_COLUMNS if k not in headers]
    
    drift_detected = False
    
    if unknown_columns:
        print(f"[ALERT] Drift Detected: Found undocumented columns in rules.csv: {unknown_columns}")
        drift_detected = True
        
    if missing_columns:
        print(f"[ALERT] Drift Detected: Expected columns missing from rules.csv: {missing_columns}")
        drift_detected = True
        
    if drift_detected:
        print("Please update the agent_policy_context.md and KNOWN_DB_COLUMNS to reflect these schema changes.")
        sys.exit(2)
    else:
        print("No drift detected. rules.csv matches expected DB schema.")
        sys.exit(0)

if __name__ == "__main__":
    check_drift()
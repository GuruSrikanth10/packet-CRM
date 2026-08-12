import re

# UUID regex
UUID_REGEX = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)

# Basic ISO-8601 / Date literal regexes
ISO_TIMESTAMP_REGEX = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
DATE_REGEX = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")

# Any digit run of 10 or more (SRN, EID, UID, Aadhaar, etc)
LONG_DIGIT_RUN_REGEX = re.compile(r"\d{10,}")

def validate_generic_text(text: str, source_values: list[str]) -> list[str]:
    """
    Check that text contains no packet-specific identifiers.
    Returns a list of violation messages. Empty list means validation passed.
    """
    violations = []
    
    if UUID_REGEX.search(text):
        violations.append("Contains a UUID.")
        
    if ISO_TIMESTAMP_REGEX.search(text) or DATE_REGEX.search(text):
        violations.append("Contains a timestamp or date literal.")
        
    if LONG_DIGIT_RUN_REGEX.search(text):
        violations.append("Contains a digit run of 10 or more (e.g. SRN, EID).")
        
    for val in source_values:
        if val and len(val) > 3 and val in text:  # Ignore very short strings that might coincidentally match
            violations.append(f"Contains source value: {val}")
            
    return violations

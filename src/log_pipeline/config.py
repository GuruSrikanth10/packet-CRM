"""
Log Reduction Pipeline - Configuration and Constants.

All tunables for the 6-stage pipeline live here so they can be adjusted
without touching pipeline logic.
"""
import os
import re

from src.utils.paths import CATALOG_PATH, DRAIN3_STATE_DIR  # noqa: F401

# ---------------------------------------------------------------------------
# Stage 2 -- ERROR branch
# ---------------------------------------------------------------------------
# How many INFO lines preceding the first ERROR to keep as context.
ERROR_CONTEXT_LINES = int(os.environ.get("LOG_ERROR_CONTEXT_LINES", "200"))

# How many lines after the last ERROR to keep as trailing context. Without a
# cap, a cascading failure keeps everything from the first error to the end
# of the trace, which is effectively the whole log (1.11).
ERROR_TRAILING_LINES = int(os.environ.get("LOG_ERROR_TRAILING_LINES", "200"))

# ---------------------------------------------------------------------------
# Stage 3 -- Drain3 clustering
# ---------------------------------------------------------------------------
# Path where the Drain3 TemplateMiner persists its parse tree between runs.
# Keeping it stable across invocations is what gives us stable template IDs.
# Re-exported from utils.paths rather than re-derived with a third
# dirname(dirname(dirname(...))) walk (G13).

# ---------------------------------------------------------------------------
# Stage 4 -- Evidence assembly guardrails
# ---------------------------------------------------------------------------
# Templates whose per-flow count is below this threshold are always kept in
# full (with example lines), never collapsed to count-only.
RARE_TEMPLATE_THRESHOLD = int(os.environ.get("LOG_RARE_TEMPLATE_THRESHOLD", "5"))

# Decision-vocabulary regex -- any raw log line matching this is *always*
# forwarded to the LLM in full text, regardless of its Drain3 cluster
# classification.  Build this from your domain; err on the side of inclusion.
DECISION_VOCABULARY_REGEX = re.compile(
    os.environ.get(
        "LOG_DECISION_VOCAB_REGEX",
        r"(?i)"
        r"(?:approved|rejected|denied|final.?decision|rule\s.*triggered"
        r"|score.?threshold|validation.?failed|dedup.*reject"
        r"|packet.*status|enrolment.*result|biometric.*match"
        r"|MAN_DEDUP|operator.*reject|quality.*check.*fail)",
    )
)

# ---------------------------------------------------------------------------
# Stage 0 -- Offline template catalog
# ---------------------------------------------------------------------------
# Path to the persisted catalog JSON built by `build_catalog.py`.
# Also re-exported from utils.paths (G13).

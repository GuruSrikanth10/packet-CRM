"""
Log Reduction Pipeline -- Top-level Orchestrator.

Wires Stages 1-4 together and returns a compact, evidence-preserving
JSON string ready for LLM injection.

Flow:
  Stage 1 (fetch) -> Stage 2 (branch on ERROR)
    -> ERROR path:   trim to error + context, format, return
    -> Normal path:  Stage 3 (cluster) -> Stage 4 (guardrails) -> format, return
"""
import json
import os

from src.log_pipeline.catalog import TemplateCatalog
from src.log_pipeline.fetcher import fetch_logs
from src.log_pipeline.reducer import branch_on_error, cluster_logs, apply_evidence_guardrails
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


_cached_catalog = None


def _get_catalog() -> TemplateCatalog:
    """Return a module-level cached catalog instance."""
    global _cached_catalog
    if _cached_catalog is None:
        _cached_catalog = TemplateCatalog()
    return _cached_catalog


def reduce_logs(event_id: str) -> str:
    """Run the full log reduction pipeline for an event_id.

    Returns a formatted string suitable for LLM context injection.
    Raw logs are also persisted to disk for audit.
    """
    # Load catalog (cached -- only reads disk once)
    catalog = _get_catalog()

    # ------------------------------------------------------------------
    # Stage 1: Fetch
    # ------------------------------------------------------------------
    raw_logs = fetch_logs(event_id, catalog=catalog)

    if not raw_logs:
        return f"No logs found for ID: {event_id}"

    total_fetched = len(raw_logs)
    log = logger.bind(event_id=event_id)
    log.info(f"[PIPELINE] Fetched {total_fetched} logs.")

    # Persist raw logs to disk for audit
    log_file_path = _save_raw_logs(event_id, raw_logs)

    # If logs are small enough, return directly
    if total_fetched < 50:
        lines = [f"--- Log Trace for ID: {event_id} ---"]
        for log in raw_logs:
            lines.append(f"[{log['timestamp']}] [{log['app_name']}] [{log['level']}] {log['message']}")
        lines.append(f"--- End of Trace ({total_fetched} logs total) ---")
        formatted = "\n".join(lines)
        _save_reduced_logs(event_id, formatted)
        return formatted

    # ------------------------------------------------------------------
    # Stage 2: Branch on ERROR
    # ------------------------------------------------------------------
    branch_result = branch_on_error(raw_logs)

    if branch_result["has_error"]:
        # Stuck path: format the trimmed context directly
        formatted = _format_error_path(event_id, branch_result["payload"], total_fetched, log_file_path)
        _save_reduced_logs(event_id, formatted)
        return formatted

    # ------------------------------------------------------------------
    # Stage 3: Clustering (approve/reject path)
    # ------------------------------------------------------------------
    clusters = cluster_logs(raw_logs, catalog=catalog)

    # ------------------------------------------------------------------
    # Stage 4: Evidence guardrails
    # ------------------------------------------------------------------
    assembled = apply_evidence_guardrails(clusters, raw_logs, catalog=catalog)

    # ------------------------------------------------------------------
    # Format for LLM injection
    # ------------------------------------------------------------------
    formatted = _format_normal_path(event_id, assembled, total_fetched, log_file_path)
    _save_reduced_logs(event_id, formatted)
    return formatted


# ======================================================================
# Formatting helpers
# ======================================================================

def _format_error_path(event_id: str, trimmed_logs: list[dict],
                       total_fetched: int, log_file_path: str) -> str:
    """Format the ERROR/stuck branch output for the LLM."""
    lines = [
        f"--- ERROR DETECTED in log trace for ID: {event_id} ---",
        f"Total raw logs: {total_fetched} (saved at {log_file_path})",
        f"Showing ERROR(s) + {len(trimmed_logs)} surrounding context lines:",
        "",
    ]
    for log in trimmed_logs:
        marker = " *** " if log.get("level", "").upper() == "ERROR" else "     "
        lines.append(f"{marker}[{log['timestamp']}] [{log['level']}] {log['message']}")
    lines.append("")
    lines.append("--- End of ERROR context ---")
    return "\n".join(lines)


def _format_normal_path(event_id: str, assembled: dict,
                        total_fetched: int, log_file_path: str) -> str:
    """Format the clustered + guardrail-processed output for the LLM."""
    clusters = assembled.get("clusters", [])
    decision_lines = assembled.get("decision_vocabulary_lines", [])
    boundary_lines = assembled.get("boundary_lines", [])

    lines = [
        f"--- Reduced Log Analysis for ID: {event_id} ---",
        f"Total raw logs: {total_fetched} (saved at {log_file_path})",
        f"Compressed into {len(clusters)} structural templates.",
        f"Rare templates (count < 5) and decision-vocabulary matches are kept in full.",
        "",
    ]

    # Boundary context
    if boundary_lines:
        lines.append("== Flow Boundaries ==")
        for bl in boundary_lines:
            lines.append(f"  [{bl['timestamp']}] [{bl['level']}] {bl['message']}")
        lines.append("")

    # Decision vocabulary matches
    if decision_lines:
        lines.append(f"== Decision-Vocabulary Matches ({len(decision_lines)} lines) ==")
        for dl in decision_lines:
            lines.append(f"  [{dl['timestamp']}] [{dl['level']}] {dl['message']}")
        lines.append("")

    # Template clusters (ordered by first_seen)
    lines.append("== Template Clusters (ordered by first appearance) ==")
    for cluster in clusters:
        classification = cluster.get("classification", "unknown")
        tid = cluster.get("template_id", "?")
        count = cluster.get("count", 0)
        template = cluster.get("template", "")
        first = cluster.get("first_seen", "")
        last = cluster.get("last_seen", "")
        examples = cluster.get("examples", [])

        lines.append(f"  [{tid}] [Count:{count:4d}] [{classification}] {template}")
        lines.append(f"         first_seen={first}  last_seen={last}")
        if examples:
            for ex in examples[:3]:
                lines.append(f"         example: {ex}")
        lines.append("")

    lines.append("--- End of Reduced Log Analysis ---")
    return "\n".join(lines)


# ======================================================================
# Disk persistence
# ======================================================================

def _save_raw_logs(event_id: str, logs: list[dict]) -> str:
    """Write raw logs to disk and return the file path."""
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        log_dir = os.path.join(base_dir, "local_casesheets", f"casebook_{event_id}")
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, "raw_logs.txt")
        with open(log_file_path, "w", encoding="utf-8") as f:
            for log in logs:
                f.write(f"[{log['timestamp']}] [{log['app_name']}] [{log['level']}] {log['message']}\n")
        logger.bind(event_id=event_id).info(f"[PIPELINE] Saved raw logs to {log_file_path}")
        return log_file_path
    except Exception as e:
        logger.bind(event_id=event_id).error(f"[PIPELINE] Failed to save raw logs: {e}")
        return "Failed to save"

def _save_reduced_logs(event_id: str, reduced_text: str) -> str:
    """Write reduced logs to disk and return the file path."""
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        log_dir = os.path.join(base_dir, "local_casesheets", f"casebook_{event_id}")
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, "reduced_logs.txt")
        with open(log_file_path, "w", encoding="utf-8") as f:
            f.write(reduced_text)
        logger.bind(event_id=event_id).info(f"[PIPELINE] Saved reduced logs to {log_file_path}")
        return log_file_path
    except Exception as e:
        logger.bind(event_id=event_id).error(f"[PIPELINE] Failed to save reduced logs: {e}")
        return "Failed to save"

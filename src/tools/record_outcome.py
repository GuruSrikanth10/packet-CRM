#!/usr/bin/env python3
"""
Operator CLI: attach a ground-truth verdict to a completed investigation.

    python3 -m src.tools.record_outcome --event-id EVT-1 --verdict CORRECT
    python3 -m src.tools.record_outcome --event-id EVT-1 --verdict INCORRECT \
        --corrected-action REPLAY --notes "resident had already resubmitted"
    python3 -m src.tools.record_outcome --list-pending

Without these verdicts there is no accuracy figure, so runbooks cannot be
safely promoted and agent regressions are invisible (ENHANCEMENT_PLAN 4.1).
"""
import argparse
import getpass
import json
import sys

from dotenv import load_dotenv

# Before the storage layer and utils.paths resolve their configuration --
# otherwise CASEBOOK_STORAGE_BACKEND and LOCAL_CASESHEETS_DIR from .env are
# invisible here and every verdict is written to, or looked for in, the
# default local directory instead of the configured store.
load_dotenv()

from src.storage.base import OUTCOME_VERDICTS, TERMINAL_STATUSES  # noqa: E402
from src.utils.outcomes import (  # noqa: E402
    InvalidVerdictError,
    UnknownEventError,
    load_outcome,
    record_outcome,
)
from src.utils.paths import LOCAL_CASESHEETS_DIR  # noqa: E402


def _list_pending():
    """Terminal casebooks with no verdict yet -- the operator's work queue."""
    if not LOCAL_CASESHEETS_DIR.exists():
        print("No casebooks found.")
        return

    pending = []
    for directory in sorted(LOCAL_CASESHEETS_DIR.iterdir()):
        casebook_file = directory / "casebook.json"
        if not directory.is_dir() or not casebook_file.exists():
            continue
        try:
            casebook = json.loads(casebook_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        status = (casebook.get("packet_status") or {}).get("status")
        if status not in TERMINAL_STATUSES:
            continue

        event_id = (casebook.get("packet_metadata") or {}).get("eid")
        if not event_id or load_outcome(event_id):
            continue

        resolution = casebook.get("resolution") or {}
        pending.append((event_id, status, resolution.get("action"),
                        resolution.get("source")))

    if not pending:
        print("No investigations are awaiting a verdict.")
        return

    print(f"{len(pending)} investigation(s) awaiting a verdict:\n")
    print(f"{'EVENT ID':<40} {'STATUS':<20} {'ACTION':<16} SOURCE")
    for event_id, status, action, source in pending:
        print(f"{event_id:<40} {status:<20} {str(action):<16} {source}")


def main():
    parser = argparse.ArgumentParser(
        description="Record whether a resolution was actually correct."
    )
    parser.add_argument("--event-id", help="Event whose resolution is being judged")
    parser.add_argument("--verdict", choices=OUTCOME_VERDICTS,
                        help="Was the agent's resolution correct?")
    parser.add_argument("--notes", default="", help="Free-text context")
    parser.add_argument("--corrected-action",
                        help="What the action should have been, if INCORRECT")
    parser.add_argument("--verified-by", default=None,
                        help="Defaults to the current OS user")
    parser.add_argument("--list-pending", action="store_true",
                        help="List terminal casebooks with no verdict yet")
    args = parser.parse_args()

    if args.list_pending:
        _list_pending()
        return 0

    if not args.event_id or not args.verdict:
        parser.error("--event-id and --verdict are required (or use --list-pending)")

    try:
        outcome = record_outcome(
            event_id=args.event_id,
            verdict=args.verdict,
            verified_by=args.verified_by or getpass.getuser(),
            notes=args.notes,
            corrected_action=args.corrected_action,
        )
    except UnknownEventError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except InvalidVerdictError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    print(f"Recorded {outcome['verdict']} for {outcome['event_id']} "
          f"(source: {outcome['resolution_source']}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

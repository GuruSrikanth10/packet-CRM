#!/usr/bin/env python3
"""
Prune old casebook directories (KUBERNETES_LOGS_PLAN.md 6.6).

`local_casesheets/` grows without bound. `prune_checkpoints.py` prunes the
LangGraph checkpoint DB, but nothing has ever pruned the casesheets, and
Kubernetes log snapshots are larger and denser than the Elasticsearch
projection -- so disk exhaustion arrives sooner than it used to.

Only terminal casebooks are eligible: an in-flight or resumable investigation
must never have its evidence deleted out from under it.

Usage:
    python3 -m src.tools.prune_casesheets --dry-run
    python3 -m src.tools.prune_casesheets --older-than-days 30
    python3 -m src.tools.prune_casesheets --older-than-days 30 --logs-only
"""
import argparse
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

TERMINAL_STATUSES = {
    "COMPLETED", "REJECTED", "NEEDS_MANUAL_REVIEW",
    "FAILED_PERMANENT", "DLQ", "FAILED_TIMEOUT",
}

#: Bulky evidence files, safe to drop while keeping the casebook itself.
LOG_ARTEFACTS = ("raw_logs.txt", "reduced_logs.txt", "raw_logs_k8s.jsonl")


def _casebook_status(directory: Path):
    casebook = directory / "casebook.json"
    if not casebook.exists():
        return None
    try:
        return json.loads(casebook.read_text(encoding="utf-8")) \
            .get("packet_status", {}).get("status")
    except Exception:
        return None


def _directory_size(directory: Path) -> int:
    return sum(f.stat().st_size for f in directory.rglob("*") if f.is_file())


def main():
    parser = argparse.ArgumentParser(
        description="Prune terminal casebook directories older than N days."
    )
    parser.add_argument("--older-than-days", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be removed without removing it.")
    parser.add_argument("--logs-only", action="store_true",
                        help="Delete only bulky log artefacts, keeping casebook.json "
                             "and status.json for audit.")
    parser.add_argument("--root", type=str, help="Override the casesheets root.")
    args = parser.parse_args()

    root = Path(args.root) if args.root else (
        Path(__file__).resolve().parent.parent.parent / "local_casesheets"
    )
    if not root.is_dir():
        print(f"Nothing to do: {root} does not exist.")
        return 0

    cutoff = time.time() - (args.older_than_days * 86400)
    eligible, skipped_active, skipped_recent, freed = [], 0, 0, 0

    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        if directory.stat().st_mtime > cutoff:
            skipped_recent += 1
            continue

        status = _casebook_status(directory)
        if status not in TERMINAL_STATUSES:
            # No casebook, or a non-terminal one: the investigation may still
            # be running or resumable, so its evidence stays.
            skipped_active += 1
            continue

        eligible.append((directory, status))
        freed += _directory_size(directory)

    print(f"Casesheets root : {root}")
    print(f"Older than      : {args.older_than_days} days")
    print(f"Eligible        : {len(eligible)}")
    print(f"Skipped (recent): {skipped_recent}")
    print(f"Skipped (active or non-terminal): {skipped_active}")
    print(f"Reclaimable     : {freed / 1024 / 1024:.1f} MiB")

    if not eligible:
        return 0

    if args.dry_run:
        print("\nDRY RUN -- would remove:")
        for directory, status in eligible[:50]:
            print(f"  {directory.name} [{status}]")
        if len(eligible) > 50:
            print(f"  ... and {len(eligible) - 50} more")
        return 0

    removed = 0
    for directory, _status in eligible:
        try:
            if args.logs_only:
                for name in LOG_ARTEFACTS:
                    target = directory / name
                    if target.exists():
                        target.unlink()
                        removed += 1
            else:
                shutil.rmtree(directory)
                removed += 1
        except Exception as e:
            print(f"  failed to remove {directory.name}: {e}", file=sys.stderr)

    scope = "log artefacts" if args.logs_only else "casebook directories"
    print(f"\nRemoved {removed} {scope}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

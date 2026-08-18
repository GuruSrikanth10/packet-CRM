#!/usr/bin/env python3
"""Phase 9 of DLT_PLAN.md -- read the DLT analysis output.

The goal is two commands from "what is failing most this week" to a specific
stack trace:

    python -m src.tools.dlt_report --top
    python -m src.tools.dlt_report --group <fingerprint-prefix>

Also:
    --case <case_id>    one case in full, with its trace
    --unreviewed        recommendations awaiting human review -- the queue a
                        person will eventually work, and the reason nothing
                        writes `final` in v1
    --stats             corpus-level counts, including the LLM-call reduction
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.dlt import groups  # noqa: E402
from src.dlt.case_storage import get_dlt_storage  # noqa: E402
from src.dlt.reuse import llm_calls_avoided  # noqa: E402

CLASS_LABELS = {
    "A": "business error",
    "B": "code defect",
    "C": "technical/transient",
    "U": "unclassified",
}


def _when(value) -> str:
    if not value:
        return "-"
    return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _resolve(prefix: str):
    """Find a group by fingerprint prefix, so nobody types 64 hex characters."""
    matches = [g for g in groups.list_groups()
               if g.get("fingerprint", "").startswith(prefix)]
    if not matches:
        raise SystemExit(f"No group whose fingerprint starts with '{prefix}'.")
    if len(matches) > 1:
        print(f"'{prefix}' matches {len(matches)} groups:")
        for group in matches:
            print(f"  {group['fingerprint'][:16]}  {group.get('signature')}")
        raise SystemExit("Use a longer prefix.")
    return matches[0]


def cmd_top(limit: int) -> None:
    all_groups = groups.list_groups()
    if not all_groups:
        print("No DLT groups recorded yet.")
        return

    ranked = sorted(all_groups, key=lambda g: -(g.get("occurrence_count") or 0))[:limit]

    print(f"{'COUNT':>6}  {'CLS':<3}  {'STATE':<8}  {'LAST SEEN':<17}  SIGNATURE")
    print("-" * 100)
    for group in ranked:
        print(f"{group.get('occurrence_count', 0):>6}  "
              f"{group.get('failure_class', '?'):<3}  "
              f"{group.get('recommendation_state', 'none'):<8}  "
              f"{_when(group.get('last_seen')):<17}  "
              f"{group.get('signature', '')[:60]}")
    print(f"\n{len(all_groups)} distinct failure signatures. "
          f"Inspect one with --group <fingerprint prefix>.")


def cmd_group(prefix: str) -> None:
    group = _resolve(prefix)

    print(f"Fingerprint : {group['fingerprint']}")
    print(f"Signature   : {group.get('signature')}")
    print(f"Class       : {group.get('failure_class')} "
          f"({CLASS_LABELS.get(group.get('failure_class'), 'unknown')})")
    print(f"Code        : {group.get('business_code') or '-'}")
    print(f"Occurrences : {group.get('occurrence_count')}")
    print(f"First seen  : {_when(group.get('first_seen'))}")
    print(f"Last seen   : {_when(group.get('last_seen'))}")

    history = group.get("corroboration_history") or {}
    if history:
        print("\nCorroboration history:")
        for verdict, count in sorted(history.items(), key=lambda kv: -kv[1]):
            print(f"  {count:5d}  {verdict}")
        if history.get("CONTRADICTED"):
            print("  NOTE: some occurrences contradicted the declared exception.")

    recommendation = group.get("recommendation")
    print(f"\nRecommendation ({group.get('recommendation_state', 'none')}):")
    if recommendation:
        print(f"  action        : {recommendation.get('action')}")
        print(f"  confidence    : {recommendation.get('confidence')}")
        print(f"  narrative     : {(recommendation.get('narrative') or '')[:400]}")
        print(f"  recommendation: {(recommendation.get('recommendation') or '')[:400]}")
        if recommendation.get("discrepancy"):
            print(f"  DISCREPANCY   : {recommendation['discrepancy']}")
    else:
        print("  (none recorded)")

    members = group.get("members") or []
    print(f"\nMembers ({len(members)} retained of {group.get('occurrence_count')}):")
    for case_id in members[-10:]:
        print(f"  {case_id}")
    if members:
        print(f"\nInspect one with --case {members[-1]}")


def cmd_case(case_id: str) -> None:
    storage = get_dlt_storage()
    casebook = storage.load(case_id)
    if not casebook:
        raise SystemExit(f"No casebook for '{case_id}'.")

    print(json.dumps(casebook, indent=2, ensure_ascii=False))

    trace = storage.load_artifact(case_id, "trace.txt")
    if trace:
        print("\n--- trace.txt ---")
        print(trace[:8000])


def cmd_unreviewed() -> None:
    """The queue a human will work. Nothing writes `final` in v1, so every
    recommendation in use is here."""
    pending = [g for g in groups.list_groups()
               if g.get("recommendation_state") == groups.STATE_DRAFT]
    if not pending:
        print("No draft recommendations awaiting review.")
        return

    pending.sort(key=lambda g: -(g.get("occurrence_count") or 0))
    print(f"{len(pending)} draft recommendation(s) awaiting review, "
          f"most-served first:\n")
    for group in pending:
        served = group.get("occurrence_count", 0)
        print(f"  {group['fingerprint'][:16]}  served to {served:>5} case(s)  "
              f"{group.get('signature', '')[:55]}")
    print("\nEach of these is being reused unreviewed. A wrong one is served "
          "to every subsequent occurrence.")


def cmd_stats() -> None:
    all_groups = groups.list_groups()
    if not all_groups:
        print("No DLT groups recorded yet.")
        return

    messages = sum(g.get("occurrence_count", 0) for g in all_groups)
    by_class = {}
    verdicts = {}
    for group in all_groups:
        cls = group.get("failure_class", "U")
        by_class[cls] = by_class.get(cls, 0) + group.get("occurrence_count", 0)
        for verdict, count in (group.get("corroboration_history") or {}).items():
            verdicts[verdict] = verdicts.get(verdict, 0) + count

    class_a_groups = [g for g in all_groups if g.get("failure_class") == "A"]
    class_a_messages = sum(g.get("occurrence_count", 0) for g in class_a_groups)

    print(f"Cases analysed        : {messages}")
    print(f"Distinct signatures   : {len(all_groups)}")
    print("\nBy class:")
    for cls in ("A", "B", "C", "U"):
        count = by_class.get(cls, 0)
        share = f"{100 * count / messages:.1f}%" if messages else "-"
        print(f"  {cls} {CLASS_LABELS[cls]:<20} {count:>7}  {share:>7}")

    if verdicts:
        print("\nCorroboration verdicts:")
        for verdict, count in sorted(verdicts.items(), key=lambda kv: -kv[1]):
            print(f"  {verdict:<15} {count:>7}")
        contradicted = verdicts.get("CONTRADICTED", 0) + verdicts.get("PARTIAL", 0)
        if contradicted:
            print(f"\n  {contradicted} case(s) where the logs did not support the "
                  f"declared exception.\n  These are the findings a developer "
                  f"cannot get from Kafka UI.")

    if class_a_messages:
        saving = llm_calls_avoided(class_a_messages, len(class_a_groups))
        print(f"\nCost model: {class_a_messages} Class A cases across "
              f"{len(class_a_groups)} signatures.")
        print(f"  Reuse avoided ~{saving * 100:.0f}% of LLM calls.")


def main():
    parser = argparse.ArgumentParser(description="Inspect DLT analysis output")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--top", action="store_true",
                       help="Failure signatures ranked by volume")
    group.add_argument("--group", metavar="FINGERPRINT",
                       help="One signature in detail (prefix is enough)")
    group.add_argument("--case", metavar="CASE_ID", help="One case in full")
    group.add_argument("--unreviewed", action="store_true",
                       help="Draft recommendations awaiting human review")
    group.add_argument("--stats", action="store_true", help="Corpus-level counts")
    parser.add_argument("--limit", type=int, default=20, help="Rows for --top")
    args = parser.parse_args()

    if args.top:
        cmd_top(args.limit)
    elif args.group:
        cmd_group(args.group)
    elif args.case:
        cmd_case(args.case)
    elif args.unreviewed:
        cmd_unreviewed()
    else:
        cmd_stats()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Operator CLI: resolution accuracy by reason code, enrolment type, and source.

    python3 -m src.tools.accuracy_report
    python3 -m src.tools.accuracy_report --json
    python3 -m src.tools.accuracy_report --min-samples 10

This is the figure that gates everything in Phase E: a runbook should not be
promoted to `serve` for a reason code until its accuracy matches the agents'
on that same code (ENHANCEMENT_PLAN 4.1, 4.2).
"""
import argparse
import json
import sys

from src.utils.outcomes import iter_outcomes, summarise


def main():
    parser = argparse.ArgumentParser(description="Report resolution accuracy.")
    parser.add_argument("--json", action="store_true", help="Emit JSON")
    parser.add_argument("--min-samples", type=int, default=1,
                        help="Hide rows with fewer outcomes than this")
    parser.add_argument("--reason-code", help="Filter to one reason code")
    args = parser.parse_args()

    summary = summarise(iter_outcomes())
    rows = [r for r in summary["rows"] if r["total"] >= args.min_samples]
    if args.reason_code:
        rows = [r for r in rows if r["reason_code"] == args.reason_code]

    if args.json:
        print(json.dumps({"rows": rows, "total_outcomes": summary["total_outcomes"]},
                         indent=2))
        return 0

    if not rows:
        print("No outcomes recorded yet.")
        print("Record some with: python3 -m src.tools.record_outcome --list-pending")
        return 0

    print(f"{'REASON CODE':<34} {'TYPE':<8} {'SOURCE':<10} "
          f"{'N':>5} {'OK':>5} {'BAD':>5} {'PART':>5} {'ACC':>7}")
    print("-" * 88)
    for row in rows:
        print(
            f"{row['reason_code'][:33]:<34} "
            f"{str(row['enrolment_type'])[:7]:<8} "
            f"{row['resolution_source'][:9]:<10} "
            f"{row['total']:>5} {row['CORRECT']:>5} {row['INCORRECT']:>5} "
            f"{row['PARTIAL']:>5} {row['accuracy']:>7.1%}"
        )
    print("-" * 88)
    print(f"{summary['total_outcomes']} outcome(s) recorded.")

    # Where the same reason code has both agent and runbook results, show them
    # side by side -- that comparison is the runbook promotion gate.
    by_code = {}
    for row in rows:
        by_code.setdefault((row["reason_code"], row["enrolment_type"]), {})[
            row["resolution_source"]] = row
    comparisons = {k: v for k, v in by_code.items()
                   if "agent" in v and "runbook" in v}
    if comparisons:
        print("\nAgent vs runbook on the same reason code:")
        for (code, etype), sources in sorted(comparisons.items()):
            agent, runbook = sources["agent"], sources["runbook"]
            delta = runbook["accuracy"] - agent["accuracy"]
            verdict = "runbook OK" if delta >= 0 else "runbook WORSE"
            print(f"  {code} ({etype}): agent {agent['accuracy']:.1%} "
                  f"(n={agent['total']}) vs runbook {runbook['accuracy']:.1%} "
                  f"(n={runbook['total']})  -> {verdict}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

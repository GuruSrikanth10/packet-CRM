#!/usr/bin/env python3
"""
Stage 0 -- Offline Template Catalog Builder.

Run this script periodically (or whenever logging changes) to build the
template catalog that powers the pipeline's noise classification.

Usage:
    python -m src.tools.build_catalog --refids-file refids.txt
    python -m src.tools.build_catalog --refids id1 id2 id3

The script fetches logs for each refid, clusters them with Drain3, computes
cross-flow statistics, and outputs the catalog JSON.
"""
import argparse
import json
import os
import sys

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.log_pipeline.catalog import TemplateCatalog
from src.log_pipeline.config import CATALOG_PATH, DECISION_VOCABULARY_REGEX
from src.log_pipeline.fetcher import fetch_logs
from src.log_pipeline.reducer import cluster_logs


def main():
    parser = argparse.ArgumentParser(description="Build the offline template catalog (Stage 0).")
    parser.add_argument("--refids", nargs="+", help="List of refids to sample.")
    parser.add_argument("--refids-file", type=str, help="Path to a file with one refid per line.")
    parser.add_argument("--output", type=str, default=CATALOG_PATH, help="Output catalog JSON path.")
    args = parser.parse_args()

    # Collect refids
    refids = []
    if args.refids:
        refids = args.refids
    elif args.refids_file:
        with open(args.refids_file, "r") as f:
            refids = [line.strip() for line in f if line.strip()]
    else:
        print("ERROR: Provide either --refids or --refids-file.")
        parser.print_help()
        sys.exit(1)

    print(f"[BUILD_CATALOG] Processing {len(refids)} refids...")

    # Track template presence across flows
    # template_id -> { "template": str, "flows_present": set(), "total_count": int }
    template_stats: dict[str, dict] = {}
    total_flows = len(refids)

    for i, refid in enumerate(refids):
        print(f"\n--- [{i+1}/{total_flows}] Processing refid: {refid} ---")
        try:
            raw_logs = fetch_logs(refid, catalog=None)  # No catalog filtering during build
            if not raw_logs:
                print(f"  No logs found for {refid}, skipping.")
                continue

            clusters = cluster_logs(raw_logs, catalog=None)

            for cluster in clusters:
                tid = cluster["template_id"]
                template_text = cluster["template"]

                if tid not in template_stats:
                    template_stats[tid] = {
                        "template": template_text,
                        "flows_present": set(),
                        "total_count": 0,
                    }
                template_stats[tid]["flows_present"].add(refid)
                template_stats[tid]["total_count"] += cluster["count"]

        except Exception as e:
            print(f"  ERROR processing {refid}: {e}")
            continue

    # Build catalog with classifications
    catalog = TemplateCatalog(path=args.output)

    for tid, stats in template_stats.items():
        pct = len(stats["flows_present"]) / total_flows if total_flows > 0 else 0
        template_text = stats["template"]

        # Classification logic
        if DECISION_VOCABULARY_REGEX.search(template_text):
            classification = "decision-marker"
            correlation = "decision"
        elif pct >= 0.90:
            classification = "boilerplate"
            correlation = "none"
        else:
            classification = "informative"
            correlation = "varies"

        catalog.upsert(
            template_id=tid,
            template=template_text,
            classification=classification,
            seen_in_pct_of_flows=pct,
            outcome_correlation=correlation,
        )

    catalog.save()
    print(f"\n[BUILD_CATALOG] Done. Catalog saved to {args.output} with {catalog.size} templates.")


if __name__ == "__main__":
    main()

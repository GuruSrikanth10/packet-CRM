#!/usr/bin/env python3
import os
import json
import time
import requests
import argparse
from filelock import FileLock

def main():
    parser = argparse.ArgumentParser(description="Approve Human-in-the-Loop Replays")
    parser.add_argument("--approve-all", action="store_true", help="Approve all matching replays without prompting")
    parser.add_argument("--filter-category", type=str, help="Only process replays with this category")
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    queue_file = os.path.join(base_dir, "db", "pending_replays.jsonl")
    lock_file = queue_file + ".lock"
    
    if not os.path.exists(queue_file):
        print("No pending replays found.")
        return

    # Load environment variables for the URL
    # Assuming python-dotenv is used, but we'll try to load it manually if not
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(base_dir), ".env"))
    except ImportError:
        pass
        
    base_url = os.environ.get("OIS_FEIGN_BASE_URL", "http://10.10.79.62:31261/ois/hold/v1")
    endpoint = f"{base_url}/api/v1/forceReplay"
    ois_api_key = os.environ.get("OIS_API_KEY")
    headers = {"X-API-Key": ois_api_key} if ois_api_key else {}
    if not ois_api_key:
        print("Warning: OIS_API_KEY is not set; replay calls will be sent unauthenticated.")

    print(f"Loading pending replays from {queue_file}...")

    replays = []
    replay_raw_lines = []
    with FileLock(lock_file, timeout=10):
        with open(queue_file, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    replays.append(json.loads(stripped))
                    replay_raw_lines.append(stripped)

    if not replays:
        print("Queue is empty.")
        return

    # Track which raw lines were actually consumed (successfully replayed,
    # or explicitly discarded) so the final rewrite below only removes those
    # -- anything appended to the queue file by a live investigation while
    # this interactive loop is running must survive (1.7/1.8).
    consumed_raw_lines = set()

    for i, replay in enumerate(replays):
        payload = replay.get("payload", {})
        packet_id = payload.get("id", "UNKNOWN")
        category = payload.get("category", "")
        raw_line = replay_raw_lines[i]

        if args.filter_category and args.filter_category.lower() != category.lower():
            continue

        print("\n" + "="*50)
        print(f"Replay Request {i+1}/{len(replays)}")
        print(f"Timestamp: {replay.get('timestamp')}")
        print(f"Packet ID: {packet_id}")
        print(f"Category: {category}")
        print(f"Priority: {payload.get('priority')}")
        print("="*50)

        if args.approve_all:
            action = 'y'
            print("Auto-approving due to --approve-all flag.")
        else:
            action = input("Approve and execute this replay? (y/n/skip): ").strip().lower()

        if action == 'y':
            print(f"Firing HTTP POST to {endpoint}...")
            try:
                # Sent as a JSON body with an auth header rather than query
                # params -- query params land in server access logs, which
                # would leak notificationEmail/notificationMobile there (1.8).
                response = requests.post(endpoint, json=payload, headers=headers, timeout=10)
                response.raise_for_status()
                print(f"Success: {response.text}")
                consumed_raw_lines.add(raw_line)
            except Exception as e:
                print(f"Failed: {e}")
                print("Keeping in queue to try again later.")
        elif action == 'n':
            print("Discarding request.")
            consumed_raw_lines.add(raw_line)
        else:
            print("Skipping request.")

    # Rewrite the queue, keeping every entry that wasn't consumed above.
    # Re-read fresh (rather than reusing the stale `replays` from the
    # initial read) so anything a live investigation queued via
    # queue_for_replay while this interactive loop was running survives (1.8).
    with FileLock(lock_file, timeout=10):
        with open(queue_file, "r", encoding="utf-8") as f:
            current_lines = f.readlines()
        remaining_lines = [ln for ln in current_lines if ln.strip() not in consumed_raw_lines]
        with open(queue_file, "w", encoding="utf-8") as f:
            f.writelines(remaining_lines)

    print("\nQueue processing complete.")

if __name__ == "__main__":
    main()

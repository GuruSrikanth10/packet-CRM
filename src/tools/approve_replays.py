#!/usr/bin/env python3
import os
import json
import time
import requests
from filelock import FileLock

def main():
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

    print(f"Loading pending replays from {queue_file}...")
    
    replays = []
    with FileLock(lock_file, timeout=10):
        with open(queue_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    replays.append(json.loads(line))
                    
    if not replays:
        print("Queue is empty.")
        return

    remaining_replays = []
    
    for i, replay in enumerate(replays):
        payload = replay.get("payload", {})
        packet_id = payload.get("id", "UNKNOWN")
        
        print("\n" + "="*50)
        print(f"Replay Request {i+1}/{len(replays)}")
        print(f"Timestamp: {replay.get('timestamp')}")
        print(f"Packet ID: {packet_id}")
        print(f"Category: {payload.get('category')}")
        print(f"Priority: {payload.get('priority')}")
        print("="*50)
        
        action = input("Approve and execute this replay? (y/n/skip): ").strip().lower()
        
        if action == 'y':
            print(f"Firing HTTP POST to {endpoint}...")
            try:
                response = requests.post(endpoint, params=payload, timeout=10)
                response.raise_for_status()
                print(f"✅ Success: {response.text}")
            except Exception as e:
                print(f"❌ Failed: {e}")
                print("Keeping in queue to try again later.")
                remaining_replays.append(replay)
        elif action == 'n':
            print("Discarding request.")
        else:
            print("Skipping request.")
            remaining_replays.append(replay)
            
    # Write back remaining replays
    with FileLock(lock_file, timeout=10):
        with open(queue_file, "w", encoding="utf-8") as f:
            for r in remaining_replays:
                f.write(json.dumps(r) + "\n")
                
    print("\nQueue processing complete.")

if __name__ == "__main__":
    main()

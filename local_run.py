import json
import sys
import os
import requests

def run_local(file_path: str):
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    print(f"Read packet {data.get('eventId')}. Sending to internal FastAPI endpoint...")
    
    try:
        response = requests.post("http://localhost:8000/process-rejection", json=data)
        response.raise_for_status()
        print("Success! Processed successfully.")
        print(response.json())
    except Exception as e:
        print(f"Error communicating with local server: {e}")
        print("Make sure you have started the server with `python src/main.py` first!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python local_run.py path/to/packet.json")
        sys.exit(1)
        
    run_local(sys.argv[1])

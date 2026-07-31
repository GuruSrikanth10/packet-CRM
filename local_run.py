import json
import sys
import os
from src.main import MessagePayload, process_message
import asyncio

async def run_local(file_path: str):
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    # Validate payload
    payload = MessagePayload.model_validate(data)
    
    # Process it directly bypassing Kafka
    await process_message(payload)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python local_run.py path/to/packet.json")
        sys.exit(1)
        
    asyncio.run(run_local(sys.argv[1]))

import uuid
from typing import Dict, Any
from faststream import FastStream
from faststream.kafka import KafkaBroker
from pydantic import BaseModel, Field

import os
import json
from .config import settings
from .agent.graph import run_investigation

LOCAL_CASESHEETS_DIR = os.path.join(os.getcwd(), "local_casesheets")
os.makedirs(LOCAL_CASESHEETS_DIR, exist_ok=True)

broker = KafkaBroker(settings.kafka_brokers)
app = FastStream(broker)

class PacketExecutionSummary(BaseModel):
    packetStatus: str
    errorData: list = []

class MessagePayload(BaseModel):
    eventId: str
    packetExecutionSummary: PacketExecutionSummary

    class Config:
        extra = "allow"

@broker.subscriber(settings.kafka_topic)
async def process_message(msg: MessagePayload):
    """Consume messages from Kafka and process rejection packets."""
    print(f"Received message: {msg.eventId} with status: {msg.packetExecutionSummary.packetStatus}")
    
    # 1. Check if it is a rejection packet
    if msg.packetExecutionSummary.packetStatus != "REJECTED":
        print(f"Message {msg.eventId} is not a rejection packet. Skipping.")
        return
        
    print(f"Processing rejection packet: {msg.eventId}")
    
    # 2. Check if casesheet already exists locally
    local_file_path = os.path.join(LOCAL_CASESHEETS_DIR, f"{msg.eventId}.json")
    if os.path.exists(local_file_path):
        print(f"Casesheet for {msg.eventId} already exists locally. Skipping investigation.")
        return
        
    # 3. Run the LangGraph investigation agent
    print(f"Starting agent investigation for {msg.eventId}...")
    try:
        casesheet = run_investigation(msg.eventId, msg.model_dump())
        print(f"Agent investigation complete for {msg.eventId}.")
        
        # 4. Write the generated casesheet to local file
        with open(local_file_path, "w", encoding="utf-8") as f:
            json.dump(casesheet, f, indent=2)
        print(f"Successfully wrote casesheet for {msg.eventId} to {local_file_path}.")
            
    except Exception as e:
        print(f"Error during agent investigation for {msg.eventId}: {e}")

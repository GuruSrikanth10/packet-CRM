import uuid
from typing import Dict, Any
from faststream import FastStream
from faststream.kafka import KafkaBroker
from pydantic import BaseModel, Field

from .config import settings
from .s3_service import check_casesheet_exists, upload_casesheet
from .agent.graph import run_investigation

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
    
    # 2. Check if casesheet already exists in S3
    if check_casesheet_exists(msg.eventId):
        print(f"Casesheet for {msg.eventId} already exists in S3. Skipping investigation.")
        return
        
    # 3. Run the LangGraph investigation agent
    print(f"Starting agent investigation for {msg.eventId}...")
    try:
        casesheet = run_investigation(msg.eventId, msg.model_dump())
        print(f"Agent investigation complete for {msg.eventId}.")
        
        # 4. Upload the generated casesheet to S3
        success = upload_casesheet(msg.eventId, casesheet)
        if success:
            print(f"Successfully uploaded casesheet for {msg.eventId} to S3.")
        else:
            print(f"Failed to upload casesheet for {msg.eventId}.")
            
    except Exception as e:
        print(f"Error during agent investigation for {msg.eventId}: {e}")

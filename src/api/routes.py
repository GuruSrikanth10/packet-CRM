import os
import json
import re
from fastapi import APIRouter
from src.models.schemas import MessagePayload
from src.core.agent_orchestrator import get_agent

router = APIRouter()

from pathlib import Path

# Fix: 3 dirname levels to reach project root because routes.py is inside src/api/
# Using Path and abspath to ensure bulletproof Windows compatibility
BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_CASESHEETS_DIR = BASE_DIR / "local_casesheets"

@router.post("/process-rejection")
def process_rejection(signal: MessagePayload):
    """
    Endpoint that receives only rejected signals from Kafka.
    It provisions the Agent to investigate the error and decides on the solution.
    """
    signal_dict = signal.model_dump()
    event_id = str(signal.eventId).strip()
    
    investigation_dir = LOCAL_CASESHEETS_DIR / f"casebook_{event_id}"
    os.makedirs(str(investigation_dir), exist_ok=True)
    
    print(f"\n[API] ⚙️ Processing Rejection for Event ID: {event_id}")
    print(f"[API] 🧠 Initializing LangGraph Agent Ecosystem...")
    
    agent = get_agent()
    
    # Run the agent
    print(f"[API] 🚀 Dispatching payload to Deterministic Graph for analysis...")
    result = agent.invoke(
        {"payload": signal_dict},
        config={"configurable": {"thread_id": event_id}}
    )
    
    print(f"[API] ✅ Agent investigation complete! Extracting Synthesis...")
    final_message = result.get("synthesis", "{}")
    
    try:
        # Fix: support matching both JSON objects {} and arrays []
        fenced_match = re.search(r"```(?:json)?\s*([\{\[].*?[\}\]])\s*```", final_message, re.DOTALL)
        if fenced_match:
            investigation_result = json.loads(fenced_match.group(1))
        else:
            investigation_result = json.loads(final_message)
    except Exception:
        investigation_result = final_message
    
    # Extract metadata safely
    packet_meta = signal_dict.get("packetMetaData") or {}
    flow_meta = signal_dict.get("flowMetaData") or {}
    exec_summary = signal_dict.get("packetExecutionSummary") or {}
    
    # Extract rejection code safely
    error_data = exec_summary.get("errorData") or []
    rejection_code = None
    for err in error_data:
        if err and err.get("errorReasonCode"):
            rejection_code = err.get("errorReasonCode")
            break
            
    # Handle investigation result defaults if it's not a dict
    if not isinstance(investigation_result, dict):
        investigation_result = {"Rejection_description": str(investigation_result)}
    
    casebook_data = {
        "Metadata - Packet Details": {
            "SRN": packet_meta.get("srn"),
            "EID": event_id,
            "REFID": packet_meta.get("refId"),
            "SOURCE": signal_dict.get("sourceTopic"),
            "PACKET_TYPE": packet_meta.get("enrolmentType"),
            "MBU": None,  # MBU mapping not immediately available in payload
            "Update_type": None,  # B/D mapping not immediately available
            "is_child": None,  # Age determination not immediately available
            "Created_at": signal_dict.get("eventTimestamp"),
            "Uploaded_at": signal_dict.get("eventTimestamp")
        },
        "Packet Status": {
            "Status": exec_summary.get("packetStatus"),
            "Service": flow_meta.get("stage"),
            "sub_service": flow_meta.get("subStage"),
            "last_updated": None,
            "Inprocess": False if exec_summary.get("packetStatus") == "REJECTED" else None,
            "Rejection Data": {
                "Rejection_code": rejection_code,
                "Rejection_description": investigation_result.get("Rejection_description"),
                "Rejection_logs": investigation_result.get("Rejection_logs"),
                "Artifact_design": investigation_result.get("Artifact_design")
            }
        },
        "Resolution": {
            "Synthesis": investigation_result.get("Synthesis"),
            "Action": investigation_result.get("Action"),
            "Resident_action": investigation_result.get("Resident_action"),
            "UIDAI_ACTION": investigation_result.get("UIDAI_ACTION")
        }
    }
    
    casebook_file = investigation_dir / "casebook.json"
    with open(casebook_file, "w") as f:
        json.dump(casebook_data, f, indent=4)
        
    print(f"[API] 💾 Successfully saved finalized Casebook to: {casebook_file}")
    
    return {"status": "processed", "casebook_path": str(casebook_file)}

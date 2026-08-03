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
    investigation_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Triggering RejectionManagerAgent for {event_id}...")
    
    agent = get_agent()
    
    # Run the agent
    result = agent.invoke(
        {"messages": [{"role": "user", "content": f"Investigate this rejected packet: {json.dumps(signal_dict)}"}]},
        config={"configurable": {"thread_id": event_id}}
    )
    
    final_message = result["messages"][-1].content
    
    try:
        # Fix: support matching both JSON objects {} and arrays []
        fenced_match = re.search(r"```(?:json)?\s*([\{\[].*?[\}\]])\s*```", final_message, re.DOTALL)
        if fenced_match:
            investigation_result = json.loads(fenced_match.group(1))
        else:
            investigation_result = json.loads(final_message)
    except Exception:
        investigation_result = final_message
    
    casebook_data = {
        "event_id": event_id,
        "original_signal": signal_dict,
        "investigation_result": investigation_result
    }
    
    casebook_file = investigation_dir / "casebook.json"
    with open(casebook_file, "w") as f:
        json.dump(casebook_data, f, indent=4)
        
    print(f"Saved investigation to {casebook_file}")
    
    return {"status": "processed", "casebook_path": str(casebook_file)}

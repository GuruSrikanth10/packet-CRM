import json
import os
import threading
from pathlib import Path
from uuid import uuid4
from fastapi import FastAPI
from contextlib import asynccontextmanager

from utils.kafkaConsumer import consume_forever
from tools.tool_registry import get_tool_by_name

# deepagents imports
from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver

# Local directory for casebooks instead of S3
LOCAL_CASESHEETS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "local_casesheets")

# Load manager context
manager_context = ""
with open(os.path.join(os.path.dirname(__file__), "manager.md"), "r") as f:
    manager_context = f.read()

# Load subagents from agents.json
subagents = []
agents_json_path = os.path.join(os.path.dirname(__file__), "agents.json")
with open(agents_json_path, "r") as f:
    agent_dictionary = json.load(f)

for entry in agent_dictionary:
    name = str(entry.get("name", "")).strip()
    prompt_file_path = os.path.join(os.path.dirname(__file__), str(entry.get("skills", "")).strip())
    available_tools = [get_tool_by_name(val) for val in entry.get("tools", [])]
    system_prompt = Path(prompt_file_path).read_text(encoding="utf-8")
    
    subagents.append({
        "name": name,
        "description": f"Agent for investigating {name}",
        "system_prompt": system_prompt,
        "tools": available_tools,
    })

# Setup LLM and deep agent (Using OpenAI since ollama is not in requirements)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
checkpointer = MemorySaver()

# Dummy context schema required by deepagents
class InvestigationContext:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

agent = create_deep_agent(
    name="RejectionManagerAgent",
    model=llm,
    system_prompt=manager_context,
    tools=[],
    subagents=subagents,
    checkpointer=checkpointer,
    context_schema=InvestigationContext,
)

def run_kafka_consumer():
    try:
        consume_forever()
    except Exception as e:
        print(f"Kafka consumer stopped due to error: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Spawn Kafka consumer thread
    consumer_thread = threading.Thread(target=run_kafka_consumer, daemon=True)
    consumer_thread.start()
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/process-rejection")
def process_rejection(signal: dict):
    event_id = signal.get("eventId", str(uuid4()))
    
    # Store directly in local_casesheets/
    investigation_dir = os.path.join(LOCAL_CASESHEETS_DIR, f"casebook_{event_id}")
    Path(investigation_dir).mkdir(parents=True, exist_ok=True)
    
    agent_prompt = f"Please investigate the following rejected packet:\n{json.dumps(signal, indent=2)}"
    
    thread_config = {"configurable": {"thread_id": event_id}}
    
    print(f"Invoking deepagents manager for event {event_id}...")
    result = agent.invoke(
        {"messages": [{"role": "user", "content": agent_prompt}]},
        config=thread_config,
        version="v2",
        context=InvestigationContext(
            investigation_dir=investigation_dir,
            event_id=event_id
        ),
    )
    
    final_message = result["messages"][-1].content
    
    casebook_data = {
        "event_id": event_id,
        "original_signal": signal,
        "investigation_result": final_message
    }
    
    casebook_file = os.path.join(investigation_dir, "casebook.json")
    with open(casebook_file, "w", encoding="utf-8") as f:
        json.dump(casebook_data, f, indent=2)
        
    print(f"Casebook written locally to {casebook_file}. S3 integration is disabled.")
    
    return {"status": "processed", "event_id": event_id, "casebook": casebook_data}

if __name__ == '__main__':
    import uvicorn
    os.chdir(os.path.dirname(__file__))
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

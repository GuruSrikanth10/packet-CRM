import os
from deepagents import create_deep_agent
from langgraph.checkpoint.memory import MemorySaver
from src.utils.llm_utils import get_llm
from src.utils.env import get_bool_env
from src.tools.tool_registry import get_tool_by_name

_agent = None

def get_agent():
    global _agent
    if _agent is not None:
        print("[ORCHESTRATOR] ⚡ Returning cached DeepAgents instance.")
        return _agent

    print("[ORCHESTRATOR] 🏗️ Building DeepAgents graph from scratch...")
    base_dir = os.path.dirname(os.path.dirname(__file__))
    
    # Pre-register tools
    available_tools = [
        get_tool_by_name("lookup_resident_database"),
        get_tool_by_name("lookup_error_code"),
        get_tool_by_name("lookup_rule_by_reason_code"),
        get_tool_by_name("add_learning_rule"),
        get_tool_by_name("fetch_elastic_logs"),
        get_tool_by_name("fetch_kubernetes_logs")
    ]
    
    # We could also read the JSON and map tool names to the actual python tools dynamically
    # For now we'll just provide all possible tools in the `shared_tools` list
    import json
    with open(os.path.join(base_dir, "config", "agents.json"), "r") as f:
        agents_config = json.load(f)
        
    enable_log_fetching = get_bool_env("ENABLE_LOG_FETCHING", False)
    if not enable_log_fetching:
        agents_config = [ac for ac in agents_config if ac["name"] != "LogFetcherAgent"]
        
    subagents = []
    for ac in agents_config:
        prompt_path = os.path.join(base_dir, ac["skills"])
        with open(prompt_path, "r", encoding="utf-8") as pf:
            system_prompt = pf.read()
            
        subagent_tools = [get_tool_by_name(t) for t in ac.get("tools", [])]
        
        subagents.append({
            "name": ac["name"],
            "description": f"Subagent {ac['name']}",
            "system_prompt": system_prompt,
            "tools": subagent_tools
        })

    llm = get_llm("complex")
    checkpointer = MemorySaver()

    manager_prompt_path = os.path.join(base_dir, "prompts", "manager.md")
    with open(manager_prompt_path, "r", encoding="utf-8") as mf:
        manager_system_prompt = mf.read()

    _agent = create_deep_agent(
        name="RejectionManagerAgent",
        model=llm,
        system_prompt=manager_system_prompt,
        tools=[],  # Manager uses no direct tools
        subagents=subagents,
        checkpointer=checkpointer
    )
    
    print("[ORCHESTRATOR] ✅ DeepAgents graph successfully constructed!")
    return _agent

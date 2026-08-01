import os
from deepagents import create_deep_agent
from langgraph.checkpoint.memory import MemorySaver
from src.utils.llm_utils import get_llm
from src.tools.tool_registry import get_tool_by_name

_agent = None

def get_agent():
    global _agent
    if _agent is not None:
        return _agent

    base_dir = os.path.dirname(os.path.dirname(__file__))
    
    # Pre-register tools
    available_tools = [
        get_tool_by_name("lookup_resident_database"),
        get_tool_by_name("lookup_error_code"),
        get_tool_by_name("lookup_rule_by_reason_code"),
        get_tool_by_name("add_learning_rule")
    ]
    
    # We could also read the JSON and map tool names to the actual python tools dynamically
    # For now we'll just provide all possible tools in the `shared_tools` list
    import json
    with open(os.path.join(base_dir, "config", "agents.json"), "r") as f:
        agents_config = json.load(f)
        
    for ac in agents_config:
        ac["tools"] = available_tools

    llm = get_llm("complex")
    checkpointer = MemorySaver()

    _agent = create_deep_agent(
        llm=llm,
        manager_prompt_path=os.path.join(base_dir, "prompts", "manager.md"),
        subagents_config=agents_config,
        checkpointer=checkpointer,
        shared_tools=available_tools
    )
    
    return _agent

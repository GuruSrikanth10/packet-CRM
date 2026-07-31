import json
from typing import Literal
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_ollama import ChatOllama
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode

from .state import AgentState
from .tools import get_agent_tools
from ..config import settings

# Initialize the LLM (ensure Ollama is running)
llm = ChatOllama(model=settings.llm_model, base_url=settings.llm_base_url)

# Bind tools to the LLM
tools = get_agent_tools()
llm_with_tools = llm.bind_tools(tools)

def investigate_node(state: AgentState):
    """The main reasoning node that decides whether to call tools or finish."""
    messages = state.get("messages", [])
    
    # If this is the first execution, add the system prompt and rejection context
    if not messages:
        payload_str = json.dumps(state["rejection_payload"], indent=2)
        system_prompt = SystemMessage(
            content="You are an investigation agent. Your job is to analyze rejection payloads, "
                    "use tools to find the root cause, and formulate a fix."
        )
        human_msg = HumanMessage(content=f"Analyze this rejection payload and investigate the cause:\n{payload_str}")
        messages = [system_prompt, human_msg]
        
    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}

def generate_casesheet_node(state: AgentState):
    """Extracts the final analysis and creates the casesheet dictionary."""
    messages = state["messages"]
    
    # Simple extraction of final text to put into the casesheet
    # In a production app, we would use structured output from the LLM here
    final_response = messages[-1].content if hasattr(messages[-1], 'content') else str(messages[-1])
    
    casesheet = {
        "message_id": state["message_id"],
        "original_payload": state["rejection_payload"],
        "investigation_summary": final_response,
        "status": "INVESTIGATED"
    }
    return {"casesheet": casesheet}

def should_continue(state: AgentState) -> Literal["tools", "generate_casesheet"]:
    """Routing logic to determine if the LLM called a tool."""
    messages = state["messages"]
    last_message = messages[-1]
    # If the LLM makes a tool call, we route to the "tools" node
    if last_message.tool_calls:
        return "tools"
    # Otherwise, we route to generate the casesheet
    return "generate_casesheet"

# Build the Graph
workflow = StateGraph(AgentState)

workflow.add_node("agent", investigate_node)
workflow.add_node("tools", ToolNode(tools))
workflow.add_node("generate_casesheet", generate_casesheet_node)

# Set the entry point
workflow.set_entry_point("agent")

# Add edges
workflow.add_conditional_edges(
    "agent",
    should_continue,
    {
        "tools": "tools",
        "generate_casesheet": "generate_casesheet"
    }
)

# Return to the agent after tool execution
workflow.add_edge("tools", "agent")
workflow.add_edge("generate_casesheet", END)

# Compile the graph
agent_graph = workflow.compile()

def run_investigation(message_id: str, payload: dict) -> dict:
    """Run the agent workflow for a given payload and return the resulting casesheet."""
    initial_state = {
        "message_id": message_id,
        "rejection_payload": payload,
        "messages": [],
        "casesheet": None
    }
    
    # Run the graph
    result = agent_graph.invoke(initial_state)
    return result.get("casesheet", {})

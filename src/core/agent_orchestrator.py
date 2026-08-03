import os
import json
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent

from src.utils.llm_utils import get_llm
from src.utils.env import get_bool_env
from src.tools.tool_registry import get_tool_by_name

class GraphState(TypedDict):
    payload: dict
    logs: str
    investigation: str
    reviewer_feedback: str
    synthesis: str
    messages: list

_agent = None

def get_agent():
    global _agent
    if _agent is not None:
        print("[ORCHESTRATOR] ⚡ Returning cached Deterministic Graph.")
        return _agent

    print("[ORCHESTRATOR] 🏗️ Building Deterministic LangGraph from scratch...")
    base_dir = os.path.dirname(os.path.dirname(__file__))
    llm = get_llm("complex")
    
    def load_prompt(filename):
        with open(os.path.join(base_dir, "prompts", filename), "r", encoding="utf-8") as f:
            return f.read()
            
    investigator_prompt = load_prompt("InvestigatorAgent.md")
    reviewer_prompt = load_prompt("ReviewerAgent.md")
    synthesis_prompt = load_prompt("SynthesisAgent.md")
    
    def fetch_logs_node(state: GraphState):
        enable_log_fetching = get_bool_env("ENABLE_LOG_FETCHING", False)
        if not enable_log_fetching:
            print("[NODE] ⏭️ Log fetching is disabled.")
            return {"logs": "Log fetching disabled."}
            
        print("[NODE] 🔄 Fetching Elastic Logs...")
        event_id = state.get("payload", {}).get("eventId", "")
        tool = get_tool_by_name("fetch_elastic_logs")
        logs = tool.invoke(event_id)
        return {"logs": logs}
        
    def investigator_node(state: GraphState):
        print("[NODE] 🔍 Investigator analyzing packet...")
        payload = state.get("payload", {})
        logs = state.get("logs", "")
        feedback = state.get("reviewer_feedback", "")
        
        prompt = f"Kafka Payload: {json.dumps(payload)}\n\n"
        if logs and logs != "Log fetching disabled.":
            prompt += f"Elasticsearch Logs: {logs}\n\n"
        if feedback:
            prompt += f"Reviewer Feedback (You MUST fix your previous analysis): {feedback}\n\n"
            
        investigator_agent = create_react_agent(llm, tools=[get_tool_by_name("lookup_rule_by_reason_code")], state_modifier=investigator_prompt)
        res = investigator_agent.invoke({"messages": [HumanMessage(content=prompt)]})
        return {"investigation": res["messages"][-1].content}

    def reviewer_node(state: GraphState):
        print("[NODE] 🧐 Reviewer validating findings...")
        investigation = state.get("investigation", "")
        prompt = f"Validate this investigation:\n{investigation}\n\nIf it's perfect, reply with exactly 'APPROVED'. If not, explain what is wrong."
        reviewer_agent = create_react_agent(llm, tools=[get_tool_by_name("add_learning_rule")], state_modifier=reviewer_prompt)
        res = reviewer_agent.invoke({"messages": [HumanMessage(content=prompt)]})
        feedback = res["messages"][-1].content
        return {"reviewer_feedback": feedback}

    def check_approval(state: GraphState):
        feedback = state.get("reviewer_feedback", "").upper()
        if "APPROVED" in feedback:
            print("[ROUTER] ✅ Reviewer approved. Moving to Synthesis.")
            return "synthesis"
        else:
            print("[ROUTER] ❌ Reviewer rejected. Looping back to Investigator.")
            return "investigator"

    def synthesis_node(state: GraphState):
        print("[NODE] ✍️ Synthesis generating final casebook...")
        investigation = state.get("investigation", "")
        prompt = f"Create the final JSON casebook based strictly on this approved investigation:\n{investigation}"
        
        synthesis_agent = create_react_agent(llm, tools=[], state_modifier=synthesis_prompt)
        res = synthesis_agent.invoke({"messages": [HumanMessage(content=prompt)]})
        return {"synthesis": res["messages"][-1].content, "messages": res["messages"]}

    # Build Graph
    workflow = StateGraph(GraphState)
    workflow.add_node("fetch_logs", fetch_logs_node)
    workflow.add_node("investigate", investigator_node)
    workflow.add_node("review", reviewer_node)
    workflow.add_node("synthesize", synthesis_node)
    
    workflow.add_edge(START, "fetch_logs")
    workflow.add_edge("fetch_logs", "investigate")
    workflow.add_edge("investigate", "review")
    workflow.add_conditional_edges("review", check_approval, {"synthesis": "synthesize", "investigator": "investigate"})
    workflow.add_edge("synthesize", END)
    
    checkpointer = MemorySaver()
    _agent = workflow.compile(checkpointer=checkpointer)
    
    print("[ORCHESTRATOR] ✅ Deterministic Graph successfully constructed!")
    return _agent

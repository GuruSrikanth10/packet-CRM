import os
import json
import contextvars
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.sqlite import SqliteSaver
import sqlite3
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from src.utils.llm_utils import get_llm
from src.utils.env import get_bool_env
from src.tools.tool_registry import get_tool_by_name
from src.utils.resilience import retry_transient, llm_breaker
from src.utils.logging_config import get_logger
from src.utils.paths import CHECKPOINT_DB_PATH

logger = get_logger(__name__)

# Per-packet context for the add_learning_rule tool, which is now built once
# at graph-construction time instead of once per review call (2.1). Each
# packet is processed on its own dedicated thread (see routes.py), and
# contextvars are thread-local by default, so setting these at the top of
# reviewer_node is safe under concurrent packets.
_current_event_id: contextvars.ContextVar[str] = contextvars.ContextVar("current_event_id", default="unknown")
_current_investigation: contextvars.ContextVar[str] = contextvars.ContextVar("current_investigation", default="")

def is_reviewer_approved(feedback: str) -> bool:
    """Return True only if the Reviewer's verdict is an unqualified APPROVED.

    A naive `"APPROVED" in feedback.upper()` substring check also matches
    "NOT APPROVED", "DISAPPROVED", or prose like "this is not approved
    because...", silently skipping the QC loop. The Reviewer is instructed to
    reply with exactly 'APPROVED' when findings are valid, so requiring the
    (markdown/whitespace-stripped) verdict to *start with* that token is both
    correct for the happy path and closed against negation phrasing.
    """
    normalized = (feedback or "").strip().strip("*_`\"' \t\n\r").upper()
    return normalized.startswith("APPROVED")

class GraphState(TypedDict):
    payload: dict
    logs: str
    db_rule: str
    investigation: str
    reviewer_feedback: str
    synthesis: str
    messages: list
    retry_count: int

_agent = None

def get_agent():
    global _agent
    if _agent is not None:
        logger.info("Returning cached Deterministic Graph.")
        return _agent

    logger.info("Building Deterministic LangGraph from scratch...")
    base_dir = os.path.dirname(os.path.dirname(__file__))
    llm = get_llm("complex")
    simple_llm = get_llm("simple")
    
    def load_prompt(filename):
        with open(os.path.join(base_dir, "prompts", filename), "r", encoding="utf-8") as f:
            prompt = f.read()
            
        policy_path = os.path.join(os.path.dirname(base_dir), "agent_policy_context.md")
        if os.path.exists(policy_path):
            with open(policy_path, "r", encoding="utf-8") as f:
                policy = f.read()
            # Inject policy context directly into the prompt so the LLM actually sees it!
            prompt += "\n\n### GLOBAL BUSINESS POLICY CONTEXT\n" + policy
            
        return prompt
            
    investigator_prompt = load_prompt("InvestigatorAgent.md")
    reviewer_prompt = load_prompt("ReviewerAgent.md")
    synthesis_prompt = load_prompt("SynthesisAgent.md")
    
    def fetch_logs_node(state: GraphState):
        event_id = state.get("payload", {}).get("eventId", "")
        log = logger.bind(event_id=event_id)
        log.info("Log fetcher node started", state="LOG_FETCHER")
        enable_log_fetching = get_bool_env("ENABLE_LOG_FETCHING", False)
        if not enable_log_fetching:
            log.info("Log fetching disabled via .env; skipping.")
            return {"logs": "Log fetching disabled."}

        log.info("Fetching Elasticsearch traces")
        tool = get_tool_by_name("fetch_elastic_logs")
        logs = tool.invoke(event_id)
        log.info("Logs retrieved")
        return {"logs": logs}

    # Create agents once during graph construction, not per invocation.
    investigator_agent = create_react_agent(llm, tools=[])
    queue_tool = get_tool_by_name("queue_for_replay")
    synthesis_agent = create_react_agent(llm, tools=[queue_tool])

    from langchain_core.tools import tool
    from datetime import datetime
    from filelock import FileLock

    @tool
    def add_learning_rule(rule_text: str, reasoning: str) -> str:
        """Propose a new permanent rule to fix Investigator mistakes."""
        target_file = os.path.join(base_dir, "prompts", "pending_rules.jsonl")
        lock_file = target_file + ".lock"

        entry = {
            "eventId": _current_event_id.get(),
            "timestamp": datetime.now().isoformat(),
            "proposed_rule": rule_text,
            "reviewer_reasoning": reasoning,
            "investigator_original_output": _current_investigation.get(),
        }
        try:
            with FileLock(lock_file, timeout=10):
                with open(target_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry) + "\n")
            return f"Successfully queued rule for human review: {rule_text}"
        except Exception as e:
            return f"Failed to queue rule: {e}"

    # 2.1: built once here (not per-review) now that the tool reads its
    # per-packet context from contextvars instead of a closure over
    # event_id/investigation -- rebuilding a React agent (with the tool
    # schema binding that implies) on every single review call, and every
    # retry loop, was pure waste.
    # 2.2: the Reviewer is a bounded verdict task, a natural fit for the
    # cheaper "simple" tier, which was otherwise constructed and unused.
    reviewer_agent = create_react_agent(simple_llm, tools=[add_learning_rule])

    def investigator_node(state: GraphState):
        payload = state.get("payload", {})
        event_id = payload.get("eventId", "unknown")
        log = logger.bind(event_id=event_id)
        log.info("Investigator node started", state="INVESTIGATING")
        logs = state.get("logs", "")
        feedback = state.get("reviewer_feedback", "")
        db_rule = state.get("db_rule", "")
        investigation = state.get("investigation", "")

        # Optimize DB Calls: Fetch rule in Python if not already fetched
        if not db_rule:
            exec_summary = payload.get("packetExecutionSummary") or {}
            error_data = exec_summary.get("errorData") or []
            reason_code = None
            for err in error_data:
                if err and err.get("errorReasonCode"):
                    reason_code = err.get("errorReasonCode")
                    break
            
            if reason_code:
                rule_tool = get_tool_by_name("lookup_rule_by_reason_code")
                db_rule = rule_tool.invoke(reason_code)
                
                # Filter DB rule by enrolmentType if multiple rules are returned
                try:
                    packet_type = payload.get("packetMetaData", {}).get("enrolmentType", "")
                    target_type = "UPDATE" if packet_type == "U" else ("ENROLMENT" if packet_type == "E" else None)
                    
                    if target_type and db_rule and db_rule.startswith("["):
                        rules_list = json.loads(db_rule)
                        filtered_rules = []
                        for r in rules_list:
                            try:
                                rule_data = json.loads(r.get("rule_data", "{}"))
                                cond = rule_data.get("statement", {}).get("Condition", {})
                                str_eq = cond.get("StringEquals", {})
                                rule_enrol_type = str_eq.get("enrolmentType")
                                if rule_enrol_type == target_type or not rule_enrol_type:
                                    filtered_rules.append(r)
                            except Exception:
                                filtered_rules.append(r)
                        
                        if filtered_rules:
                            db_rule = json.dumps(filtered_rules)
                except Exception as e:
                    log.warning("Error filtering rules by enrolmentType", error=str(e))
            else:
                db_rule = "No errorReasonCode found in payload."

        is_retry = bool(feedback)
        if is_retry:
            # Retry: the static context (payload/logs/rule) was already sent
            # on the first attempt and hasn't changed since -- resending it
            # verbatim on every retry multiplies token cost for no benefit
            # on a multi-retry packet. Send only the delta: the prior
            # investigation plus the reviewer's feedback (2.3).
            prompt = (
                f"Your previous analysis:\n{investigation}\n\n"
                f"Reviewer Feedback (You MUST fix your previous analysis): {feedback}\n\n"
            )
        else:
            # Project the payload down to only the fields the prompt
            # actually needs -- the full nested Kafka message carries many
            # fields (sourceTopic, callbackTopic, taskMetaData, rejectBits,
            # resubmissionSummary, uidV2DataArray, ...) the Investigator
            # never uses (2.3).
            flow_meta = payload.get("flowMetaData") or {}
            projected_payload = {
                "eventId": payload.get("eventId"),
                "packetMetaData": payload.get("packetMetaData"),
                "packetExecutionSummary": payload.get("packetExecutionSummary"),
                "flowMetaData": {"stage": flow_meta.get("stage")},
            }
            prompt = f"Kafka Payload: {json.dumps(projected_payload)}\n\n"
            if logs and logs != "Log fetching disabled.":
                prompt += f"Elasticsearch Logs: {logs}\n\n"
            prompt += f"Database Rule Configuration:\n{db_rule}\n\n"

        @llm_breaker
        @retry_transient
        def invoke_investigator():
            return investigator_agent.invoke({"messages": [
                SystemMessage(content=investigator_prompt),
                HumanMessage(content=prompt)
            ]})

        res = invoke_investigator()
        log.info("Investigator finished analysis")
        return {"investigation": res["messages"][-1].content, "db_rule": db_rule}

    def reviewer_node(state: GraphState):
        investigation = state.get("investigation", "")
        event_id = state.get("payload", {}).get("eventId", "unknown")
        log = logger.bind(event_id=event_id)
        log.info("Reviewer node started", state="REVIEWING")

        # Set the per-packet context the module-scope add_learning_rule tool
        # reads (see its definition above) instead of closing over these
        # values directly.
        _current_event_id.set(event_id)
        _current_investigation.set(investigation)

        prompt = f"Validate this investigation:\n{investigation}\n\nIf it's perfect, reply with exactly 'APPROVED'. If not, explain what is wrong."

        @llm_breaker
        @retry_transient
        def invoke_reviewer():
            return reviewer_agent.invoke({"messages": [
                SystemMessage(content=reviewer_prompt),
                HumanMessage(content=prompt)
            ]})

        res = invoke_reviewer()
        feedback = res["messages"][-1].content
        log.info("Reviewer finished assessment")
        return {"reviewer_feedback": feedback, "retry_count": state.get("retry_count", 0) + 1}

    def check_approval(state: GraphState):
        feedback = state.get("reviewer_feedback", "")
        retry_count = state.get("retry_count", 0)
        max_retries = int(os.environ.get("MAX_INVESTIGATION_RETRIES", 3))
        event_id = state.get("payload", {}).get("eventId", "unknown")
        log = logger.bind(event_id=event_id, retry_count=retry_count)

        if is_reviewer_approved(feedback):
            log.info("Reviewer APPROVED findings", transition="synthesis")
            return "synthesis"
        elif retry_count >= max_retries:
            log.warning("Maximum retries reached", max_retries=max_retries, transition="escalate", state="NEEDS_MANUAL_REVIEW")
            return "escalate"
        else:
            log.info("Reviewer REJECTED findings", transition="investigator", state="RETRYING")
            return "investigator"

    def escalate_node(state: GraphState):
        event_id = state.get("payload", {}).get("eventId", "unknown")
        logger.bind(event_id=event_id).info("Generating escalation casebook", state="ESCALATING")

        # We manually construct a fake synthesis payload that forces the routes.py to mark it NEEDS_MANUAL_REVIEW
        investigation = state.get("investigation", "")
        feedback = state.get("reviewer_feedback", "")

        # We format it to match the expected JSON structure so routes.py parses it
        escalation_result = {
            "rejection_description": f"ESCALATED: The automated agents could not agree on a resolution after multiple attempts.\nLast Investigation:\n{investigation}\n\nLast Reviewer Feedback:\n{feedback}",
            "synthesis": "ESCALATED TO HUMAN REVIEW. The system encountered a complex edge case and exceeded the maximum allowed retries for agentic resolution.",
            "action": "MANUAL_REVIEW",
            "resident_action": "PENDING"
        }
        # Dump to JSON so routes.py can parse it
        return {"synthesis": json.dumps(escalation_result)}

    def synthesis_node(state: GraphState):
        event_id = state.get("payload", {}).get("eventId", "unknown")
        log = logger.bind(event_id=event_id)
        log.info("Synthesis node started", state="SYNTHESIZING")
        investigation = state.get("investigation", "")
        prompt = f"Create the final JSON casebook based strictly on this approved investigation:\n{investigation}"

        @llm_breaker
        @retry_transient
        def invoke_synthesis():
            return synthesis_agent.invoke({"messages": [
                SystemMessage(content=synthesis_prompt),
                HumanMessage(content=prompt)
            ]})

        res = invoke_synthesis()
        log.info("Synthesis finished")
        return {"synthesis": res["messages"][-1].content, "messages": res["messages"]}

    # Build Graph
    workflow = StateGraph(GraphState)
    workflow.add_node("fetch_logs", fetch_logs_node)
    workflow.add_node("investigate", investigator_node)
    workflow.add_node("review", reviewer_node)
    workflow.add_node("synthesize", synthesis_node)
    workflow.add_node("escalate", escalate_node)
    
    workflow.add_edge(START, "fetch_logs")
    workflow.add_edge("fetch_logs", "investigate")
    workflow.add_edge("investigate", "review")
    workflow.add_conditional_edges("review", check_approval, {"synthesis": "synthesize", "investigator": "investigate", "escalate": "escalate"})
    workflow.add_edge("synthesize", END)
    workflow.add_edge("escalate", END)
    
    os.makedirs(CHECKPOINT_DB_PATH.parent, exist_ok=True)
    conn = sqlite3.connect(str(CHECKPOINT_DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    checkpointer = SqliteSaver(conn)
    _agent = workflow.compile(checkpointer=checkpointer)
    
    logger.info("Deterministic Graph successfully constructed!")
    return _agent

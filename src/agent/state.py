from typing import TypedDict, Optional, List
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage
from typing_extensions import Annotated

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    message_id: str
    rejection_payload: dict
    casesheet: Optional[dict]

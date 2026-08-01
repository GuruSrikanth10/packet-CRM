from typing import Optional, List
from pydantic import BaseModel

class ErrorData(BaseModel):
    type: Optional[str] = None
    errorReasonCode: str

class PacketExecutionSummary(BaseModel):
    hasExecutionErrors: bool
    hasValidationErrors: bool
    packetStatus: str
    errorData: Optional[List[ErrorData]] = None
    isExecutionSuccess: bool
    isValidationSuccess: bool

class MessagePayload(BaseModel):
    eventId: str
    category: Optional[str] = None
    eventType: Optional[str] = None
    referenceId: Optional[str] = None
    packetExecutionSummary: PacketExecutionSummary

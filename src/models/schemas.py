from typing import Optional, List, Any, Dict
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
    eventTimestamp: Optional[str] = None
    eventVersion: Optional[str] = None
    sid: Optional[str] = None
    sidDate: Optional[str] = None
    version: Optional[str] = None
    sourceTopic: Optional[str] = None
    callbackTopic: Optional[str] = None
    flowMetaData: Optional[Dict[str, Any]] = None
    taskMetaData: Optional[Any] = None
    packetMetaData: Optional[Dict[str, Any]] = None
    packetExecutionSummary: PacketExecutionSummary
    rejectBits: Optional[Any] = None
    requestStage: Optional[str] = None
    requestStageStatus: Optional[str] = None
    resubmissionSummary: Optional[Dict[str, Any]] = None
    uidV2DataArray: Optional[Any] = None

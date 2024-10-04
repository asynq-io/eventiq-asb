from typing import Any, Optional

from azure.servicebus import ServiceBusReceivedMessage
from pydantic import BaseModel, Field


class Result(BaseModel):
    type: str
    message: ServiceBusReceivedMessage
    extras: Optional[dict[str, Any]] = Field(default_factory=dict)
    action: str

    model_config = {"arbitrary_types_allowed": True}


class Fail(Result):
    type: str = "fail"
    action: str = "dead_letter_message"


class Ack(Result):
    type: str = "ack"
    action: str = "complete_message"


class Nack(Result):
    type: str = "nack"
    action: str = "abandon_message"

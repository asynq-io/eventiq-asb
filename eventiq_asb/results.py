from dataclasses import dataclass, field
from typing import Any, Union

from azure.servicebus import ServiceBusReceivedMessage


@dataclass
class BaseResult:
    message: ServiceBusReceivedMessage
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass
class Fail(BaseResult):
    type: str = "fail"
    action: str = "dead_letter_message"


@dataclass
class Ack(BaseResult):
    type: str = "ack"
    action: str = "complete_message"


@dataclass
class Nack(BaseResult):
    type: str = "nack"
    action: str = "abandon_message"


Result = Union[Ack, Nack, Fail]

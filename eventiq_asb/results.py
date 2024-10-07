from dataclasses import dataclass, field
from typing import Any

from azure.servicebus import ServiceBusReceivedMessage


@dataclass
class Result:
    message: ServiceBusReceivedMessage
    type: str
    action: str
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass
class Fail(Result):
    type: str = "fail"
    action: str = "dead_letter_message"


@dataclass
class Ack(Result):
    type: str = "ack"
    action: str = "complete_message"


@dataclass
class Nack(Result):
    type: str = "nack"
    action: str = "abandon_message"

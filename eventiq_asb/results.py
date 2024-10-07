from dataclasses import dataclass
from typing import Union

from azure.servicebus import ServiceBusReceivedMessage


@dataclass
class BaseResult:
    message: ServiceBusReceivedMessage


@dataclass
class Fail(BaseResult):
    reason: str
    action: str = "dead_letter_message"


@dataclass
class Ack(BaseResult):
    action: str = "complete_message"


@dataclass
class Nack(BaseResult):
    action: str = "abandon_message"


Result = Union[Ack, Nack, Fail]

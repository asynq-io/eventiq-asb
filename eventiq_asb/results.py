from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Union

from azure.servicebus import ServiceBusReceivedMessage


@dataclass
class BaseResult(ABC):
    message: ServiceBusReceivedMessage

    @abstractmethod
    def dict(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass
class Fail(BaseResult):
    reason: str
    action: str = "dead_letter_message"

    def dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "action": self.action,
            "reason": self.reason,
        }


@dataclass
class Ack(BaseResult):
    action: str = "complete_message"

    def dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "action": self.action,
        }


@dataclass
class Nack(BaseResult):
    action: str = "abandon_message"

    def dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "action": self.action,
        }


Result = Union[Ack, Nack, Fail]

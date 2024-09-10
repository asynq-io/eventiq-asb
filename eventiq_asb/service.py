from typing import ClassVar

from eventiq import Service as EventiqService
from eventiq.types import MiddlewareType

from .middlewares import ReceiverMiddleware


class Service(EventiqService):
    """
    Default Eventiq Service class overwritten
    to handle ReceiverMiddleware in background thread
    """

    default_middlewares: ClassVar[list[MiddlewareType]] = [ReceiverMiddleware]

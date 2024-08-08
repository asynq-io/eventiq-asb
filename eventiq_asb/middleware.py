from eventiq import Middleware
from eventiq.consumer import Consumer
from eventiq.exceptions import Fail
from eventiq.types import CloudEventType


class DeadLetterQueueMiddleware(Middleware[CloudEventType]):
    async def after_fail_message(
        self, *, consumer: Consumer, message: CloudEventType, exc: Fail
    ) -> None:
        """
        Call self.service.publish() to send the message to a dead letter queue.
        """

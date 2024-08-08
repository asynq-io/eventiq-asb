from __future__ import annotations

from typing import TYPE_CHECKING

from eventiq import Middleware
from eventiq.types import CloudEventType

from .broker import AzureServiceBusBroker

if TYPE_CHECKING:
    from eventiq import Consumer
    from eventiq.exceptions import Fail


class DeadLetterQueueMiddleware(Middleware[CloudEventType]):
    async def after_fail_message(
        self, *, consumer: Consumer, message: CloudEventType, exc: Fail
    ) -> None:
        broker = self.service.broker
        if not isinstance(broker, AzureServiceBusBroker):
            self.logger.warning(
                "Dead letter queue middleware only works with Azure Service Bus broker"
            )
            return

        receiver = broker.get_message_receiver(message.raw)
        if not receiver:
            self.logger.warning("Message receiver not found for message %s", message.id)
            return

        await receiver.dead_letter_message(message.raw, reason=exc.reason)

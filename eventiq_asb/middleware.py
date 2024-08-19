from __future__ import annotations

from typing import TYPE_CHECKING, Any

from azure.core import exceptions
from azure.servicebus.aio import AutoLockRenewer, ServiceBusSession
from azure.servicebus.aio.management import ServiceBusAdministrationClient
from azure.servicebus.management import CorrelationRuleFilter
from eventiq.middleware import Middleware
from eventiq.types import CloudEventType
from eventiq.utils import to_float

from .broker import AzureServiceBusBroker

if TYPE_CHECKING:
    from azure.servicebus import ServiceBusReceivedMessage
    from eventiq import Consumer, Service
    from eventiq.exceptions import Fail


class AbstractServiceBusMiddleware(Middleware):
    def __init__(self, service: Service) -> None:
        super().__init__(service)
        if not isinstance(service.broker, AzureServiceBusBroker):
            error = "Unsupported broker type"
            raise TypeError(error)
        self.broker: AzureServiceBusBroker = service.broker


class DeadLetterQueueMiddleware(
    AbstractServiceBusMiddleware, Middleware[CloudEventType]
):
    async def after_fail_message(
        self, *, consumer: Consumer, message: CloudEventType, exc: Fail
    ) -> None:
        receiver = self.broker.get_message_receiver(message.raw)
        if not receiver:
            self.logger.warning("Message receiver not found for message %s", message.id)
            return

        await receiver.dead_letter_message(message.raw, reason=exc.reason)


class ServiceBusManagerMiddleware(AbstractServiceBusMiddleware):
    def __init__(self, service: Service) -> None:
        super().__init__(service)
        self.client = ServiceBusAdministrationClient.from_connection_string(
            self.broker.url
        )

    async def after_broker_connect(self) -> None:
        try:
            await self.client.create_topic(self.broker.topic_name)
            self.logger.debug("Topic %s created", self.broker.topic_name)
        except exceptions.ResourceExistsError:
            self.logger.debug("Topic %s already exists", self.broker.topic_name)

    async def before_consumer_start(self, *, consumer: Consumer) -> None:
        try:
            await self.create_subscription(subscription_name=consumer.topic)
            self.logger.debug("Subscription %s created", consumer.topic)
            await self.delete_rule(consumer.topic, "$Default")
            self.logger.debug("Default Rule %s removed", consumer.topic)
        except exceptions.ResourceExistsError:
            self.logger.debug("Subscription %s already exists", consumer.topic)
        finally:
            await self.create_rule(consumer.topic)

    async def delete_rule(self, subscription_name: str, rule_name: str) -> None:
        """
        Initial subscription rule is removed and dedicated rule for this specific filtering is added
        (check create_rule method)
        """
        await self.client.delete_rule(
            self.broker.topic_name, subscription_name, rule_name
        )

    async def create_rule(self, subscription_name: str) -> None:
        """
        Creates rule on topic and subscription with filtering by label
        which allows ASB to work as other Eventiq Brokers
        """
        try:
            await self.client.create_rule(
                topic_name=self.broker.topic_name,
                subscription_name=subscription_name,
                rule_name="label-filter",
                filter=CorrelationRuleFilter(label=subscription_name),
            )
            self.logger.debug("Rule for %s created", subscription_name)
        except exceptions.ResourceExistsError:
            self.logger.debug("Rule %s already exists", subscription_name)

    async def create_subscription(self, subscription_name: str) -> None:
        """
        Method used to create default subscription based on provided topic and subscription name.
        """
        await self.client.create_subscription(self.broker.topic_name, subscription_name)

    async def after_broker_disconnect(self) -> None:
        if self.client:
            await self.client.close()


class AutoLockRenewerMiddleware(AbstractServiceBusMiddleware):
    def __init__(self, service: Service) -> None:
        super().__init__(service)
        self._renewer: AutoLockRenewer = AutoLockRenewer()

    async def before_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEventType,
        result: Any = None,
        exc: Exception | None = None,
    ) -> None:
        max_lock_duration = (
            to_float(consumer.timeout) or self.broker.default_consumer_timeout
        )
        receiver = self.broker.get_message_receiver(message.raw)
        if not receiver:
            self.logger.warning(
                "AutoLockRemover not found receiver for message %s", message.id
            )
            return
        self._renewer.register(
            receiver=receiver,
            renewable=message.raw,
            max_lock_renewal_duration=max_lock_duration,
            on_lock_renew_failure=self._on_lock_renew_failure,
        )

    async def _on_lock_renew_failure(
        self,
        renewable: ServiceBusSession | ServiceBusReceivedMessage,
        exc: Exception | None,
    ) -> None:
        self.logger.warning(
            "Lock renewal failed for message %d: %s",
            id(renewable),
            str(renewable),
        )
        if exc:
            self.logger.warning("Lock renewal error:", exc_info=exc)

    async def after_broker_disconnect(self) -> None:
        await self._renewer.close()

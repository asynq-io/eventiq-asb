from __future__ import annotations

from typing import TYPE_CHECKING, cast

from azure.core import exceptions
from azure.servicebus.management import CorrelationRuleFilter
from eventiq.middleware import Middleware

from .broker import AzureServiceBusBroker

if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer, Service
    from eventiq.exceptions import Fail


class ServiceBusMiddleware(Middleware):
    error_msg = "Unsupported broker type"

    def __init__(self, service: Service) -> None:
        if not isinstance(service.broker, AzureServiceBusBroker):
            raise TypeError(self.error_msg)
        super().__init__(service)

    @property
    def broker(self) -> AzureServiceBusBroker:
        return cast(AzureServiceBusBroker, self.service.broker)


class DeadLetterQueueMiddleware(ServiceBusMiddleware):
    async def after_fail_message(
        self, *, consumer: Consumer, message: CloudEvent, exc: Fail
    ) -> None:
        receiver = self.broker.get_message_receiver(message.raw)
        if not receiver:
            self.logger.warning("Message receiver not found for message %s", message.id)
            return

        await receiver.dead_letter_message(message.raw, reason=exc.reason)


class ServiceBusManagerMiddleware(ServiceBusMiddleware):
    def __init__(self, service: Service) -> None:
        super().__init__(service)
        # dynamic import to avoid requiring aiohttp
        from azure.servicebus.aio.management import ServiceBusAdministrationClient

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

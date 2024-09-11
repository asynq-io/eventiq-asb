from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

from azure.core import exceptions
from azure.servicebus.management import CorrelationRuleFilter
from eventiq.middleware import Middleware
from eventiq.utils import to_float, utc_now

from .broker import AzureServiceBusBroker

if TYPE_CHECKING:
    from azure.servicebus import ServiceBusReceivedMessage
    from azure.servicebus.aio import ServiceBusReceiver
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


class AutoLockRenewerMiddleware(ServiceBusMiddleware):
    def __init__(self, service: Service) -> None:
        super().__init__(service)
        self._tasks: dict[int, asyncio.Task] = {}

    async def before_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEvent,
        result: Any = None,
        exc: Exception | None = None,
    ) -> None:
        max_lock_duration = (
            to_float(consumer.timeout) or self.broker.default_consumer_timeout
        ) + 5
        receiver = self.broker.get_message_receiver(message.raw)
        if not receiver:
            self.logger.warning(
                "AutoLockRemover not found receiver for message %s", message.id
            )
            return
        task = asyncio.create_task(
            self.auto_lock_renewer(receiver, message.raw, timeout=max_lock_duration)
        )
        self._tasks[id(message.raw)] = task

    async def _cancel_task(self, message: ServiceBusReceivedMessage) -> None:
        task = self._tasks.pop(id(message), None)
        if task:
            task.cancel()

    async def before_ack(self, *, consumer: Consumer, raw_message: Any) -> None:
        await self._cancel_task(raw_message)

    async def before_nack(self, *, consumer: Consumer, raw_message: Any) -> None:
        await self._cancel_task(raw_message)

    async def before_broker_disconnect(self) -> None:
        for task in self._tasks.values():
            task.cancel()

    async def auto_lock_renewer(
        self,
        receiver: ServiceBusReceiver,
        message: ServiceBusReceivedMessage,
        timeout: float,
    ) -> None:
        deadline = utc_now() + timedelta(seconds=timeout)

        while utc_now() < deadline:
            if message.locked_until_utc:
                self.logger.debug(
                    "Message is locked until: %s", message.locked_until_utc
                )
                left = (utc_now() - message.locked_until_utc).total_seconds() - 5
                if left > 0:
                    self.logger.debug("Waiting for %d", left)
                    await asyncio.sleep(left)
            if message._settled:  # noqa: SLF001
                self.logger.debug("Message %d is settled", id(message))
                return

            if message._lock_expired:  # noqa: SLF001
                self.logger.warning("Lock expired for message %d", id(message))
                return
            try:
                self.logger.debug("Renewing lock for message %d", id(message))
                expires_at = await receiver.renew_message_lock(message)
                self.logger.debug(
                    "Renewed lock for message %d to %s", id(message), expires_at
                )
            except Exception as e:
                self.logger.warning(
                    "Error renewing lock for message %d", id(message), exc_info=e
                )
                await asyncio.sleep(5)

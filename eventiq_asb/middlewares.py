from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, cast

import anyio
from azure.core import exceptions
from azure.servicebus.aio import (
    AutoLockRenewer,
    ServiceBusReceiver,
    ServiceBusSession,
)
from azure.servicebus.management import CorrelationRuleFilter
from eventiq.middleware import Middleware
from eventiq.utils import to_float

from .broker import AzureServiceBusBroker

if TYPE_CHECKING:
    from azure.servicebus import ServiceBusReceivedMessage
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


class ReceiverMiddleware(ServiceBusMiddleware):
    def __init__(self, service: Service) -> None:
        super().__init__(service)
        self._receiver_consumers_to_start: set[Consumer] = set()
        self._receivers: dict[int, ServiceBusReceiver] = {}

    def get_receiver(
        self, raw_message: ServiceBusReceivedMessage
    ) -> ServiceBusReceiver | None:
        return self._receivers.get(id(raw_message))

    def pop_receiver(
        self, raw_message: ServiceBusReceivedMessage
    ) -> ServiceBusReceiver | None:
        return self._receivers.pop(id(raw_message), None)

    def set_receiver(
        self, receiver: ServiceBusReceiver, raw_message: ServiceBusReceivedMessage
    ) -> None:
        self._receivers[id(raw_message)] = receiver

    async def after_broker_connect(self) -> None:
        threading.excepthook = self.stop_client
        thread = threading.Thread(target=self._run)
        thread.start()

    async def after_consumer_start(self, *, consumer: Consumer) -> None:
        """
        Separate receiver in dedicated thread is required to start before starting the consumer
        """
        self._receiver_consumers_to_start.add(consumer)

    def _run(self) -> None:
        asyncio.run(self._run_receiver_in_thread())

    async def _run_receiver_in_thread(self) -> None:
        async with AutoLockRenewer() as renewer, anyio.create_task_group() as tg:
            tg.start_soon(self._results_handler)
            while True:
                if not self._receiver_consumers_to_start:
                    await anyio.sleep(0.1)
                    continue
                consumer_instance = self._receiver_consumers_to_start.pop()
                tg.start_soon(self.start_receiver, consumer_instance, renewer)

    async def start_receiver(
        self, consumer_instance: Consumer, renewer: AutoLockRenewer
    ) -> None:
        """
        Method called in background thread as task for consumer to handler receiving messages
        and register them to AutoLockRenewer before sending them to further processing
        """
        self.logger.info("Receiver for %s started", consumer_instance.topic)
        async with self.broker.client.get_subscription_receiver(
            topic_name=self.broker.topic_name,
            subscription_name=consumer_instance.topic,
        ) as receiver:
            while True:
                queue = self.broker.msgs_queues[consumer_instance.topic]
                batch = consumer_instance.concurrency - queue.qsize()
                if batch == 0:
                    await anyio.sleep(0.1)
                    continue

                received_msgs = await receiver.receive_messages(max_message_count=batch)
                self.logger.debug("Fetching %d messages", len(received_msgs))
                max_lock_renewal_duration = (
                    (
                        to_float(consumer_instance.timeout)
                        or self.broker.default_consumer_timeout
                    )
                    * consumer_instance.concurrency
                    * 3
                )  # just to be safe...
                for msg in received_msgs:
                    renewer.register(
                        receiver,
                        msg,
                        max_lock_renewal_duration=max_lock_renewal_duration,
                    )
                    self.set_receiver(receiver=receiver, raw_message=msg)
                for msg in received_msgs:
                    await queue.put(msg)

    async def _results_handler(self) -> None:
        """
        Method called in background thread as task for result such as nack/ack handling
        """
        while True:
            action, raw_message = await self.broker.ack_nack_queue.get()
            method = getattr(self, action)
            await method(raw_message)

    async def ack(self, raw_message: ServiceBusReceivedMessage) -> None:
        receiver = self.pop_receiver(raw_message)
        # retrieve receiver reference for given message
        # check if raw message is already settled else error is raised - private method
        if receiver and not raw_message._settled:  # noqa: SLF001
            await receiver.complete_message(raw_message)
        else:
            self.logger.warning(
                "Cannot nack message %d: %s, receiver reference is missing",
                id(raw_message),
                str(raw_message),
            )

    async def nack(self, raw_message: ServiceBusReceivedMessage) -> None:
        receiver = self.pop_receiver(raw_message)
        # retrieve receiver reference for given message
        # check if raw message is already settled else error is raised - private method
        if receiver and not raw_message._settled:  # noqa: SLF001
            await receiver.abandon_message(raw_message)
        else:
            self.logger.warning(
                "Cannot nack message %d: %s, receiver reference is missing",
                id(raw_message),
                str(raw_message),
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
            exc_info=exc,
        )

    def stop_client(self, exc: threading.ExceptHookArgs) -> None:
        asyncio.run(self.broker.client.close())
        self.logger.error(
            "Thread Error occurred in thread: %r. Error type: %s",
            exc.thread,
            exc.exc_type,
            exc_info=exc.exc_value,
        )

    async def after_fail_message(
        self, *, consumer: Consumer, message: CloudEvent, exc: Fail
    ) -> None:
        await self.broker.ack_nack_queue.put(("fail", message.raw))
        self.logger.error(
            "Failed message: %r for consumer: %s",
            message,
            consumer.topic,
            exc_info=exc,
        )

    async def fail(self, raw_message: ServiceBusReceivedMessage) -> None:
        receiver = self.get_receiver(raw_message)
        if receiver and not raw_message._settled:  # noqa: SLF001
            await receiver.dead_letter_message(raw_message)
        else:
            self.logger.warning(
                "Cannot move to Dead Letter Queue. Message%d: %s, receiver reference is missing",
                id(raw_message),
                str(raw_message),
            )

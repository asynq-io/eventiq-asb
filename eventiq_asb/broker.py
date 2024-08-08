from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any
from weakref import WeakValueDictionary

from azure.servicebus import ServiceBusMessage, ServiceBusReceivedMessage
from azure.servicebus.aio import (
    AutoLockRenewer,
    ServiceBusClient,
    ServiceBusReceiver,
    ServiceBusSender,
    ServiceBusSession,
)
from eventiq.broker import BulkMessage, UrlBroker
from eventiq.exceptions import BrokerError
from eventiq.settings import UrlBrokerSettings
from pydantic import AnyUrl, UrlConstraints

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from anyio.streams.memory import MemoryObjectSendStream
    from eventiq import Consumer
    from eventiq.types import ID, DecodedMessage

AzureServiceBusUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["sb", "amqp"])]


class AzureServiceBusSettings(UrlBrokerSettings[AzureServiceBusUrl]):
    topic_name: str


class AzureServiceBusBroker(UrlBroker[ServiceBusReceivedMessage, None]):
    Settings = AzureServiceBusSettings
    protocol = "sb"
    error_msg = "Broker is not connected"
    WILDCARD_ONE = "*"
    WILDCARD_MANY = "#"

    def __init__(
        self,
        topic_name: str,
        enable_lock_renewer: bool = True,
        batch_max_size: int | None = None,
        **extra: Any,
    ) -> None:
        super().__init__(**extra)
        self.topic_name = topic_name
        self.enable_lock_renewer = enable_lock_renewer
        self.batch_max_size = batch_max_size
        self._client: ServiceBusClient | None = None
        self._publisher: ServiceBusSender | None = None
        self._renever: AutoLockRenewer | None = None
        self._receivers: WeakValueDictionary[int, ServiceBusReceiver] = (
            WeakValueDictionary()
        )

    @property
    def client(self) -> ServiceBusClient:
        if self._client is None:
            raise self.connection_error
            raise BrokerError(self.error_msg)
        return self._client

    @property
    def publisher(self) -> ServiceBusSender:
        if self._publisher is None:
            raise self.connection_error
        return self._publisher

    def get_message_receiver(
        self, raw_message: ServiceBusReceivedMessage
    ) -> ServiceBusReceiver | None:
        return self._receivers.get(id(raw_message))

    @staticmethod
    def decode_message(raw_message: ServiceBusReceivedMessage) -> DecodedMessage:
        return raw_message.body, None

    @staticmethod
    def get_message_metadata(raw_message: ServiceBusReceivedMessage) -> dict[str, str]:
        return {}

    def get_num_delivered(self, raw_message: ServiceBusReceivedMessage) -> int | None:
        return raw_message.delivery_count

    @property
    def is_connected(self) -> bool:
        # implement healthcheck
        return self._client is not None

    async def connect(self) -> None:
        if self._client is None:
            self._client = ServiceBusClient.from_connection_string(self.url)
            self._publisher = self._client.get_topic_sender(self.topic_name)
            if self.enable_lock_renewer:
                self._renever = AutoLockRenewer()

    async def disconnect(self) -> None:
        if self._publisher:
            await self._publisher.close()
        if self._renever:
            await self._renever.close()
        if self._client:
            await self._client.close()

    def should_nack(self, raw_message: ServiceBusReceivedMessage) -> bool:
        return (
            raw_message.delivery_count is not None and raw_message.delivery_count <= 3
        )

    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        **kwargs: Any,
    ) -> None:
        msg = self._build_message(topic, body, headers=headers, **kwargs)
        await self.publisher.send_messages(msg)

    async def bulk_publish(
        self,
        messages: list[BulkMessage],
        topic: str | None = None,
    ) -> None:
        batch_message = await self.publisher.create_message_batch(self.batch_max_size)
        for message in messages:
            message_topic = topic or message.topic
            msg = self._build_message(
                message_topic,
                message.body,
                headers=message.headers,
                **message.kwargs,
            )
            batch_message.add_message(msg)
        await self.publisher.send_messages(batch_message)

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream[ServiceBusReceivedMessage],
    ) -> None:
        receiver = self.client.get_subscription_receiver(
            topic_name=consumer.topic, subscription_name=group
        )
        prefetch_count = consumer.options.get(
            "prefetch_count", consumer.concurrency * 2
        )
        max_wait_time = consumer.options.get("max_wait_time", 5)
        async with receiver, send_stream:
            while True:
                received_msgs = await receiver.receive_messages(
                    max_message_count=prefetch_count, max_wait_time=max_wait_time
                )
                for msg in received_msgs:
                    self._receivers[id(msg)] = (
                        receiver  # store weak reference to receiver for ack/nack
                    )
                    if self._renever is not None:
                        self._renever.register(
                            receiver,
                            msg,
                            max_lock_renewal_duration=100,
                            on_lock_renew_failure=self._on_lock_renew_failure,
                        )  # register message for lock renewal if enabled
                    await send_stream.send(msg)

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

    async def ack(self, raw_message: ServiceBusReceivedMessage) -> None:
        receiver = self._receivers.pop(
            id(raw_message), None
        )  # retrieve receiver reference for given message
        if receiver:
            await receiver.complete_message(raw_message)
        else:
            self.logger.warning(
                "Cannot ack message %d: %s, receiver reference is missing",
                id(raw_message),
                str(raw_message),
            )

    async def nack(
        self, raw_message: ServiceBusReceivedMessage, delay: int | None = None
    ) -> None:
        receiver = self._receivers.pop(
            id(raw_message), None
        )  # retrieve receiver reference for given message
        if receiver:
            await receiver.abandon_message(raw_message)
        else:
            self.logger.warning(
                "Cannot ack message %d: %s, receiver reference is missing",
                id(raw_message),
                str(raw_message),
            )

    @staticmethod
    def _build_message(
        topic: str,
        body: bytes,
        *,
        message_id: ID | None = None,
        session_id: str | None = None,
        message_content_type: str | None = None,
        time_to_live: timedelta | None = None,
        scheduled_enqueue_time_utc: datetime | None = None,
        correlation_id: str | None = None,
        partition_key: str | None = None,
        to: str | None = None,
        reply_to: str | None = None,
        reply_to_session_id: str | None = None,
        headers: dict[str, str],
        **kwargs: Any,
    ) -> ServiceBusMessage:
        return ServiceBusMessage(
            body,
            subject=topic,
            application_properties=dict(**headers),
            session_id=session_id,
            message_id=str(message_id) if message_id else None,
            content_type=message_content_type,
            correlation_id=correlation_id,
            partition_key=partition_key,
            to=to,
            reply_to=reply_to,
            reply_to_session_id=reply_to_session_id,
            scheduled_enqueue_time_utc=scheduled_enqueue_time_utc,
        )

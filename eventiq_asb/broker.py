from __future__ import annotations

from typing import TYPE_CHECKING, Any

import anyio
from azure.servicebus import ServiceBusMessage, ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver, ServiceBusSender
from eventiq.broker import BulkMessage, UrlBroker

from .settings import AzureServiceBusSettings

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from anyio.streams.memory import MemoryObjectSendStream
    from eventiq import Consumer
    from eventiq.types import ID, DecodedMessage


class AzureServiceBusBroker(UrlBroker[ServiceBusReceivedMessage, None]):
    Settings = AzureServiceBusSettings
    protocol = "sb"
    error_msg = "Broker is not connected"
    WILDCARD_ONE = "*"
    WILDCARD_MANY = "#"

    def __init__(
        self,
        topic_name: str,
        batch_max_size: int | None = None,
        **extra: Any,
    ) -> None:
        super().__init__(**extra)
        self.topic_name = topic_name
        self.batch_max_size = batch_max_size
        self._client: ServiceBusClient | None = None
        self._publisher: ServiceBusSender | None = None
        self._receivers: dict[int, ServiceBusReceiver] = {}
        self._publisher_lock = anyio.Lock()

    @property
    def client(self) -> ServiceBusClient:
        if self._client is None:
            raise self.connection_error
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
        return next(raw_message.body), None

    @staticmethod
    def get_message_metadata(raw_message: ServiceBusReceivedMessage) -> dict[str, str]:
        return {}

    def get_num_delivered(self, raw_message: ServiceBusReceivedMessage) -> int | None:
        return raw_message.delivery_count

    @property
    def is_connected(self) -> bool:
        if self._publisher:
            self._publisher._check_live()  # noqa: SLF001
            return True
        return False

    async def connect(self) -> None:
        if self._client is None:
            self._client = ServiceBusClient.from_connection_string(self.url)
            self._publisher = self._client.get_topic_sender(self.topic_name)

    async def disconnect(self) -> None:
        if self._publisher:
            await self._publisher.close()
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
        async with self._publisher_lock:
            msg = self._build_message(topic, body, headers=headers, **kwargs)
            await self.publisher.send_messages(msg)

    async def bulk_publish(
        self,
        messages: list[BulkMessage],
        topic: str | None = None,
    ) -> None:
        batch_message = await self.publisher.create_message_batch(self.batch_max_size)
        for message in messages:
            msg = self._build_message(
                message.topic,
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
            topic_name=self.topic_name, subscription_name=consumer.topic
        )
        max_wait_time = consumer.options.get("max_wait_time", 5)
        async with receiver, send_stream:
            while True:
                batch = consumer.concurrency - len(
                    send_stream._state.buffer  # noqa: SLF001
                )
                if batch == 0:
                    await anyio.sleep(0.1)
                    continue

                received_msgs = await receiver.receive_messages(
                    max_message_count=batch, max_wait_time=max_wait_time
                )
                self.logger.debug("Fetching %d messages", batch)
                for msg in received_msgs:
                    self._receivers[id(msg)] = (
                        receiver  # store weak reference to receiver for ack/nack
                    )
                    await send_stream.send(msg)

    async def ack(self, raw_message: ServiceBusReceivedMessage) -> None:
        receiver = self._receivers.pop(
            id(raw_message), None
        )  # retrieve receiver reference for given message
        # check if raw message is already settled else error is raised - private method
        if receiver and not raw_message._settled:  # noqa: SLF001
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
        if receiver and not raw_message._settled:  # noqa: SLF001
            await receiver.abandon_message(raw_message)
        else:
            self.logger.warning(
                "Cannot nack message %d: %s, receiver reference is missing",
                id(raw_message),
                str(raw_message),
            )

    @staticmethod
    def _build_message(
        topic: str,
        body: bytes,
        *,
        message_id: ID,
        message_content_type: str,
        session_id: str | None = None,
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
            application_properties=dict(headers.items()),
            session_id=session_id,
            message_id=str(message_id),
            content_type=message_content_type,
            correlation_id=correlation_id,
            partition_key=partition_key,
            to=to,
            reply_to=reply_to,
            reply_to_session_id=reply_to_session_id,
            scheduled_enqueue_time_utc=scheduled_enqueue_time_utc,
        )

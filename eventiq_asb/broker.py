from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import anyio
from azure.servicebus import ServiceBusMessage, ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusClient, ServiceBusSender
from eventiq import Service
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
        from .middlewares import ReceiverMiddleware

        Service.default_middlewares.append(ReceiverMiddleware)
        self.topic_name = topic_name
        self.batch_max_size = batch_max_size
        self._client: ServiceBusClient | None = None
        self._publisher: ServiceBusSender | None = None
        self._publisher_lock = anyio.Lock()
        self.msgs_queues: dict[str, asyncio.Queue] = {}
        self.ack_nack_queue: asyncio.Queue = asyncio.Queue()

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

    @staticmethod
    def decode_message(raw_message: ServiceBusReceivedMessage) -> DecodedMessage:
        return next(raw_message.body), None

    @staticmethod
    def get_message_metadata(raw_message: ServiceBusReceivedMessage) -> dict[str, str]:
        return {}

    def get_num_delivered(self, raw_message: ServiceBusReceivedMessage) -> int | None:
        if isinstance(raw_message.delivery_count, int):
            return raw_message.delivery_count + 1
        return None

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
        if delivery_count := self.get_num_delivered(raw_message):
            return delivery_count <= 3
        return False

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
        self.msgs_queues[consumer.topic] = asyncio.Queue(maxsize=consumer.concurrency)
        async with send_stream:
            while True:
                raw_message = await self.msgs_queues[consumer.topic].get()
                await send_stream.send(raw_message)

    async def ack(self, raw_message: ServiceBusReceivedMessage) -> None:
        self.logger.debug(
            "Message with id %s sent to ack queue to Receiver Instance", id(raw_message)
        )
        await self.ack_nack_queue.put(("ack", raw_message))

    async def nack(
        self, raw_message: ServiceBusReceivedMessage, delay: int | None = None
    ) -> None:
        self.logger.debug(
            "Message with id %s sent to nack queue to Receiver Instance",
            id(raw_message),
        )
        await self.ack_nack_queue.put(("nack", raw_message))

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

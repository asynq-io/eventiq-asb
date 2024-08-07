from __future__ import annotations

from typing import Annotated

from anyio.streams.memory import MemoryObjectSendStream
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusClient
from eventiq import CloudEvent, Consumer
from eventiq.broker import UrlBroker
from eventiq.exceptions import BrokerError
from eventiq.settings import UrlBrokerSettings
from eventiq.types import Encoder
from pydantic import AnyUrl, UrlConstraints

AzureServiceBusUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["sb", "amqp"])]


class AzureServiceBusSettings(UrlBrokerSettings[AzureServiceBusUrl]):
    pass


class AzureServiceBusBroker(UrlBroker[ServiceBusReceivedMessage, None]):
    Settings = AzureServiceBusSettings
    protocol = "sb"

    WILDCARD_ONE = "*"
    WILDCARD_MANY = "#"

    def __init__(self, **extra) -> None:
        super().__init__(**extra)
        self._client: ServiceBusClient | None = None

    @property
    def client(self) -> ServiceBusClient:
        if self._client is None:
            raise BrokerError("Broker is not connected")
        return self._client

    @staticmethod
    def get_message_data(raw_message: ServiceBusReceivedMessage) -> bytes:
        return raw_message.body

    @staticmethod
    def get_message_metadata(raw_message: ServiceBusReceivedMessage) -> dict[str, str]:
        return {}

    def get_num_delivered(self, raw_message: ServiceBusReceivedMessage) -> int | None:
        return raw_message.delivery_count

    @property
    def is_connected(self) -> bool:
        return self._client is not None

    async def connect(self) -> None:
        if self._client is None:
            self._client = ServiceBusClient.from_connection_string(self.url)

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()

    def should_nack(self, raw_message: ServiceBusReceivedMessage) -> bool:
        return (
            raw_message.delivery_count is not None and raw_message.delivery_count <= 3
        )

    async def ack(self, raw_message: ServiceBusReceivedMessage) -> None:
        ...

    async def nack(
        self, raw_message: ServiceBusReceivedMessage, delay: int | None = None
    ):
        ...

    async def publish(
        self, message: CloudEvent, encoder: Encoder | None = None, **kwargs
    ) -> None:
        ...

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream[ServiceBusReceivedMessage],
    ):
        ...

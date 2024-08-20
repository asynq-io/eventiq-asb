from typing import Annotated, Union

from eventiq.settings import UrlBrokerSettings
from pydantic import AnyUrl, UrlConstraints
from typing_extensions import TypeAlias

ServiceBusSharedAccessKey: TypeAlias = str
ServiceBusUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["sb", "amqp"])]
ServiceBusConnectionString = Union[ServiceBusUrl, ServiceBusSharedAccessKey]


class AzureServiceBusSettings(UrlBrokerSettings[ServiceBusConnectionString]):
    topic_name: str

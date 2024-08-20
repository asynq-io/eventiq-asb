from typing import Annotated, Union

from eventiq.settings import UrlBrokerSettings
from pydantic import AnyUrl, StringConstraints, UrlConstraints
from typing_extensions import TypeAlias

ServiceBusSharedAccessKey: TypeAlias = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        pattern=r"^Endpoint=sb:\/\/(.+?)\.servicebus\.windows\.net\/;SharedAccessKeyName=(.+?);SharedAccessKey=(.+)$",
    ),
]

ServiceBusUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["sb", "amqp"])]
ServiceBusConnectionString = Union[ServiceBusUrl, ServiceBusSharedAccessKey]


class AzureServiceBusSettings(UrlBrokerSettings[ServiceBusConnectionString]):
    topic_name: str

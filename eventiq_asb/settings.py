from typing import Annotated

from eventiq.settings import UrlBrokerSettings
from pydantic import AnyUrl, Field, UrlConstraints

AzureServiceBusUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["sb", "amqp"])]


class AzureServiceBusSettings(UrlBrokerSettings[AzureServiceBusUrl]):
    topic_name: str = Field(alias="TOPIC_NAME")
    url: str = Field(alias="BROKER_URL")


asb_settings = AzureServiceBusSettings()

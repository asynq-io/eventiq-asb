import os

from eventiq import Broker, Service

from eventiq_asb import (
    AzureServiceBusBroker,
    ServiceBusManagerMiddleware,
)


def test_is_subclass():
    assert issubclass(AzureServiceBusBroker, Broker)


def test_settings():
    os.environ.update(
        {
            "BROKER_URL": "Endpoint=sb://test-domain.servicebus.windows.net/;SharedAccessKeyName=test-name;SharedAccessKey=test-key",
            "BROKER_TOPIC_NAME": "test_topic",
        }
    )
    broker = AzureServiceBusBroker.from_env()
    service = Service(name="test", broker=broker)
    service.add_middleware(ServiceBusManagerMiddleware)
    assert isinstance(broker, AzureServiceBusBroker)

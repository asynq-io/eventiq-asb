import os

from eventiq import Broker

from eventiq_asb import AzureServiceBusBroker


def test_is_subclass():
    assert issubclass(AzureServiceBusBroker, Broker)


def test_settings():
    os.environ.update(
        {
            "BROKER_URL": "sb://localhost:3333",
            "BROKER_TOPIC_NAME": "test_topic",
        }
    )
    broker = AzureServiceBusBroker.from_env()
    assert isinstance(broker, AzureServiceBusBroker)

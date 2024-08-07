import os

from eventiq import Broker

from eventiq_asb import AzureServiceBusBroker


def test_is_subclass():
    assert issubclass(AzureServiceBusBroker, Broker)


def test_settings():
    os.environ["BROKER_URL"] = "sb://localhost:3333"
    broker = AzureServiceBusBroker.from_env()
    assert isinstance(broker, AzureServiceBusBroker)

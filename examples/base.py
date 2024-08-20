from eventiq import CloudEvent, Service

from eventiq_asb import (
    AutoLockRenewerMiddleware,
    AzureServiceBusBroker,
    DeadLetterQueueMiddleware,
    ServiceBusManagerMiddleware,
)

service = Service(
    name="example-service",
    broker=AzureServiceBusBroker(
        url="sb://example.servicebus.windows.net/",
        topic_name="example-topic",
    ),
)

service.add_middleware(DeadLetterQueueMiddleware)
service.add_middleware(AutoLockRenewerMiddleware)
service.add_middleware(ServiceBusManagerMiddleware)


@service.subscribe(topic="example-topic")
async def example_consumer(message: CloudEvent):
    print(message.data)

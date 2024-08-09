from eventiq import CloudEvent, Service

from eventiq_asb import AzureServiceBusBroker, DeadLetterQueueMiddleware

service = Service(
    name="example-service",
    broker=AzureServiceBusBroker(
        topic="example-topic", url="sb://example.servicebus.windows.net/"
    ),
)

service.add_middleware(DeadLetterQueueMiddleware)


@service.subscribe(topic="example-topic")
async def example_consumer(message: CloudEvent):
    print(message.data)

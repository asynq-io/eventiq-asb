![Tests](https://github.com/asynq-io/eventiq-asb/workflows/Tests/badge.svg)
![Build](https://github.com/asynq-io/eventiq-asb/workflows/Publish/badge.svg)
![License](https://img.shields.io/github/license/asynq-io/eventiq-asb)
![Mypy](https://img.shields.io/badge/mypy-checked-blue)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json)](https://github.com/charliermarsh/ruff)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)
![Python](https://img.shields.io/pypi/pyversions/eventiq-asb)
![Format](https://img.shields.io/pypi/format/eventiq-asb)
![PyPi](https://img.shields.io/pypi/v/eventiq-asb)

# eventiq-asb

Azure Service Bus broker implementation for eventiq


## Installation

```shell
pip install eventiq-asb
```

With optional dependencies:

```shell
pip install 'eventiq-asb[aiohttp]'
```


## Usage

```python
from eventiq import CloudEvent, Service

from eventiq_asb import AzureServiceBusBroker, DeadLetterQueueMiddleware

service = Service(
    name="example-service",
    broker=AzureServiceBusBroker(
        topic_name="example-topic", url="sb://example.servicebus.windows.net/"
    ),
)

service.add_middleware(DeadLetterQueueMiddleware)

@service.subscribe(topic="example-topic")
async def example_consumer(message: CloudEvent):
    print(message.data)

```

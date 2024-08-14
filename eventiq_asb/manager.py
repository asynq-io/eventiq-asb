from azure.core import exceptions
from azure.servicebus.aio.management import ServiceBusAdministrationClient
from azure.servicebus.management import CorrelationRuleFilter
from eventiq.logging import LoggerMixin


class ServiceBusManager(LoggerMixin):
    def __init__(self, connection_string: str) -> None:
        self._service = ServiceBusAdministrationClient.from_connection_string(
            connection_string
        )

    async def create_topic(self, topic_name: str) -> None:
        try:
            await self._service.create_topic(topic_name)
            self.logger.debug("Topic %s created", topic_name)
        except exceptions.ResourceExistsError:
            self.logger.debug("Topic %s already exists", topic_name)

    async def delete_rule(
        self, topic_name: str, subscription_name: str, rule_name: str
    ) -> None:
        await self._service.delete_rule(topic_name, subscription_name, rule_name)

    async def create_rule(self, topic_name: str, subscription_name: str) -> None:
        """
        Creates rule on topic and subscription with filtering by label
        which allows ASB to work as other Eventiq Brokers
        """
        try:
            await self._service.create_rule(
                topic_name=topic_name,
                subscription_name=subscription_name,
                rule_name="label-filter",
                filter=CorrelationRuleFilter(label=subscription_name),
            )
            self.logger.debug("Rule for %s created", subscription_name)
        except exceptions.ResourceExistsError:
            self.logger.debug("Rule %s already exists", subscription_name)

    async def create_subscription(
        self, topic_name: str, subscription_name: str
    ) -> None:
        """
        Method used to create default subscription based on provided topic and subscription name.
        Initial subscription rule is removed and dedicated rule for this specific filtering is added
        (check create_rule method)
        """
        try:
            await self._service.create_subscription(topic_name, subscription_name)
            await self.delete_rule(topic_name, subscription_name, "$Default")
            self.logger.debug("Subscription %s created", subscription_name)
        except exceptions.ResourceExistsError:
            self.logger.debug("Subscription %s already exists", subscription_name)
        finally:
            await self.create_rule(topic_name, subscription_name)

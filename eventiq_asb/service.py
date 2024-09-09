from eventiq import Service

from typing import Any

import anyio
from anyio import from_thread
from pydantic import ValidationError
from .exceptions import ReceiverFinishedException

from eventiq.consumer import Consumer
from eventiq.types import Decoder, Message
from eventiq.utils import to_float
from eventiq.exceptions import DecodeError, Fail, Retry, Skip
import inspect

from anyio.abc import TaskGroup, BlockingPortal
import asyncio

class ServiceASB(Service):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._exc_queue = asyncio.Queue()


    async def run(self, enable_signal_handler: bool = True) -> None:
        async with self.lifespan(self) as state:
            if state:
                self.state.update(state)

            await self.connect()
            try:
                with from_thread.start_blocking_portal() as portal:
                    async with anyio.create_task_group() as tg:
                        if enable_signal_handler:
                            tg.start_soon(self.watch_for_signals, tg.cancel_scope)
                        await self.start_consumers(tg, portal)
            finally:
                with anyio.move_on_after(5, shield=True):
                    await self.disconnect()


    async def start_consumers(self, tg: TaskGroup, portal: BlockingPortal) -> None:
        tg.start_soon(self.exc_handler)
        for consumer in self.consumers.values():
            consumer.maybe_set_publisher(self.publish)
            await self.dispatch_before("consumer_start", consumer=consumer)

            # use asyncio.Queue instead of anyio streams because of issues in multithreaded environment
            self.broker.add_queue(consumer.topic)

            tg.start_soon(self.broker.sender, self.name, consumer)
            tg.start_soon(self.results_consumer, consumer)

            for i in range(consumer.concurrency):
                self.logger.info("Starting consumer %s task %s", consumer.name, i)
                portal.start_task_soon(
                    self.receiver,
                    consumer,
                    name=f"{consumer.name}:{i + 1}",
                )

            await self.dispatch_after("consumer_start", consumer=consumer)

    async def exc_handler(self):
        """Listen for exceptions in background thread and raise them in main thread."""

        while True:
            if self._exc_queue.empty():
                await anyio.sleep(0.1)
                continue

            exc = await self._exc_queue.get()
            raise exc

    async def results_consumer(self, consumer: Consumer) -> None:
        """consumer results from background thread and handle them in main thread"""

        while True:
            if self.broker._result_queues[consumer.topic].empty():
                await anyio.sleep(0.1)
                continue

            action, args, kwargs = await self.broker._result_queues[consumer.topic].get()
            method = getattr(self, action, None)

            try:
                if inspect.iscoroutinefunction(method):
                    await method(*args, **kwargs)
                elif callable(method):
                    method(*args, **kwargs)
                else:
                    self.logger.warning(f"Method '{action}' not found or is not callable")
            except Exception as e:
                self.logger.exception(
                    "Failed to execute method %s with kwargs %s",
                    action,
                    kwargs,
                    exc_info=e,
                )


    async def send_result(self, action: str, _consumer: Consumer, *args: Any, **kwargs: Any) -> None:
        """Send result from background thread to main thread"""

        await self.broker._result_queues[_consumer.topic].put((action, args, kwargs))

    async def receiver(
        self,
        consumer: Consumer
    ) -> None:
        """Listen for messages and send them for processing"""

        exc = None
        try:
            consumer_timeout = to_float(
                consumer.timeout or self.broker.default_consumer_timeout,
            )
            decoder = consumer.decoder or self.decoder

            while True:
                if self.broker._msg_queues[consumer.topic].empty():
                    await anyio.sleep(0.1)
                    continue

                raw_message = await self.broker._msg_queues[consumer.topic].get()

                await self._process(
                    consumer,
                    raw_message,
                    decoder,
                    consumer_timeout,
                )
        except BaseException as e:
            exc = e
        finally:
            if not exc:
                exc = ReceiverFinishedException()

            await self._exc_queue.put(exc)
            raise exc


    async def _process(
        self,
        consumer: Consumer,
        raw_message: Message,
        decoder: Decoder,
        timeout: float,
    ) -> None:
        exc: Exception | None = None
        result = None

        try:
            data, headers = self.broker.decode_message(raw_message)
            message = decoder.decode(data, consumer.event_type)
            message.set_context(self, raw_message, headers)
        except (DecodeError, ValidationError) as e:
            self.logger.exception(
                "Failed to validate message %s.",
                raw_message,
                exc_info=e,
            )
            if self.broker.should_nack(raw_message):
                await self.send_result(
                    "nack",
                    _consumer=consumer,
                    consumer=consumer,
                    message=raw_message,
                    delay=self.broker.validate_error_delay,
                )
            else:
                await self.send_result(
                    "ack",
                    _consumer=consumer,
                    consumer=consumer,
                    message=raw_message,
                )
            return

        try:
            await self.send_result(
                "dispatch_before",
                _consumer=consumer,
                event="process_message",
                consumer=consumer,
                message=message,
            )

            self.logger.info(
                "Running consumer %s with message %s", consumer.name, message.id
            )
            with anyio.fail_after(timeout):
                result = await consumer.process(message)
        except Exception as e:
            exc = e
        except anyio.get_cancelled_exc_class():
            with anyio.move_on_after(1, shield=True):
                # directly call nack skipping all middlewares
                # await self.broker.nack(raw_message)
                await self.send_result(
                    "nack",
                    _consumer=consumer,
                    consumer=consumer,
                    message=raw_message
                )
            raise

        await self._handle_message_finalization(consumer, message, result, exc)

    async def _handle_message_finalization(
        self,
        consumer: Consumer,
        message,
        result: Any,
        exc: Exception | None,
    ) -> None:
        try:
            await self.dispatch_after(
                "process_message",
                consumer=consumer,
                message=message,
                result=result,
                exc=exc,
            )
        except Exception as e:
            exc = e

        if exc is None:
            await self.send_result(
                "ack",
                _consumer=consumer,
                consumer=consumer,
                message=message.raw,
            )
            return

        if isinstance(exc, Retry):
            # retry should be handled on asb level

            await self.send_result(
                "nack",
                _consumer=consumer,
                consumer=consumer,
                message=message.raw,
                delay=exc.delay
            )
            return

        if isinstance(exc, Skip):
            await self.dispatch_after(
                "skip_message",
                consumer=consumer,
                message=message,
                exc=exc,
            )
            await self.send_result(
                "ack",
                _consumer=consumer,
                consumer=consumer,
                message=message.raw,
            )
            return

        if isinstance(exc, Fail):
            await self.dispatch_after(
                "fail_message",
                consumer=consumer,
                message=message,
                exc=exc,
            )
            await self.send_result(
                "ack",
                _consumer=consumer,
                consumer=consumer,
                message=message.raw,
            )
            return

        await self.default_action(consumer, message.raw)
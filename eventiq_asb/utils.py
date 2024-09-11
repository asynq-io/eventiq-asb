from __future__ import annotations
import anyio
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusSession, ServiceBusReceiver
from azure.servicebus._common.utils import (
    utc_now,
    get_renewable_lock_duration,
)
import datetime
from azure.servicebus.exceptions import AutoLockRenewTimeout, AutoLockRenewFailed, ServiceBusError
from azure.servicebus._common.auto_lock_renewer import SHORT_RENEW_SCALING_FACTOR
from typing import Union, Callable, Optional, Awaitable
from anyio.abc import TaskGroup
from contextlib import AsyncExitStack
from eventiq.broker import Broker

Renewable = Union[ServiceBusSession, ServiceBusReceivedMessage]
AsyncLockRenewFailureCallback = Callable[
    [Renewable, Optional[Exception]], Awaitable[None]
]

class AutoLockRenewer:

    def __init__(self, broker: Broker) -> None:
        self.broker = broker
        self._tg: None | TaskGroup = None
        self._shutdown = anyio.Event()

    async def __aenter__(self):
        if self._shutdown.is_set():
            raise ServiceBusError(
                "The AutoLockRenewer has already been shutdown. Please create a new instance for"
                " auto lock renewing."
            )

        self._exitstack = AsyncExitStack()
        await self._exitstack.__aenter__()
        self._tg = await self._exitstack.enter_async_context(
            anyio.create_task_group()
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._shutdown.set()
        return await self._exitstack.__aexit__(exc_type, exc_val, exc_tb)

    def register(
        self,
        receiver: ServiceBusReceiver,
        renewable: Renewable,
        max_lock_renewal_duration: float,
        on_lock_renew_failure: Optional[AsyncLockRenewFailureCallback] = None,
    ) -> None:
        """Register a renewable entity for automatic lock renewal.

        Args:
            - receiver: The ServiceBusReceiver instance that is associated with the message or the session to be auto-lock-renewed.
            - renewable: A locked entity that needs to be renewed.
            - max_lock_renewal_duration: A time in seconds that the lock should be maintained for
            - on_lock_renew_failure: An async callback may be specified to be called when the lock is lost on the renewable being registered.
        """

        if not isinstance(renewable, (ServiceBusReceivedMessage, ServiceBusSession)):
            raise TypeError(
                "AutoLockRenewer only supports registration of types "
                "azure.servicebus.ServiceBusReceivedMessage (via a receiver's receive methods) and "
                "azure.servicebus.aio.ServiceBusSession "
                "(via a session receiver's property receiver.session)."
            )
        if self._shutdown.is_set() or self._tg is None:
            raise ServiceBusError(
                "The AutoLockRenewer has already been shutdown or wasn't entered via async context manager."
                "Please create a new instance for auto lock renewing."
            )
        if renewable.locked_until_utc is None:
            raise ValueError(
                "Only azure.servicebus.ServiceBusReceivedMessage objects in PEEK_LOCK receive mode may"
                "be lock-renewed.  (E.g. only messages received via receive() or the receiver iterator,"
                "not using RECEIVE_AND_DELETE receive mode, and not returned from Peek)"
            )

        self._tg.start_soon(
            self._auto_lock_renew, receiver, renewable, max_lock_renewal_duration, on_lock_renew_failure
        )

    def _is_renewable(
        self, renewable: Renewable
    ) -> bool:
        # pylint: disable=protected-access
        if self._shutdown.is_set():
            return False
        if hasattr(renewable, "_settled") and renewable._settled:  # type: ignore
            return False
        if renewable._lock_expired:
            return False
        try:
            if not renewable._receiver._running:  # type: ignore
                return False
        except AttributeError:  # If for whatever reason the renewable isn't hooked up to a receiver
            raise ServiceBusError(
                "Cannot renew an entity without an associated receiver.  "
                "ServiceBusReceivedMessage and active ServiceBusReceiver.Session objects are expected."
            ) from None
        return True

    async def _auto_lock_renew(
        self,
        receiver: ServiceBusReceiver,
        renewable: Renewable,
        max_lock_renewal_duration: float,
        on_lock_renew_failure: Optional[AsyncLockRenewFailureCallback] = None,
    ) -> None:
        # pylint: disable=protected-access
        self.broker.logger.info(
            "Running async lock auto-renew for %r seconds", max_lock_renewal_duration
        )

        error, lock_expired = None, True

        try:
            with anyio.fail_after(max_lock_renewal_duration, shield=True):
                while self._is_renewable(renewable):

                    try:
                        # Renewable is a session
                        await renewable.renew_lock()  # type: ignore
                    except AttributeError:
                        # Renewable is a message
                        await receiver.renew_message_lock(renewable)  # type: ignore

                    # renew at most once every 10 seconds, at least 10 seconds before expiry
                    sleep_time = min(max(get_renewable_lock_duration(renewable).seconds - 10, 0), 10)

                    await anyio.sleep(sleep_time)

            lock_expired = renewable._lock_expired
        except TimeoutError:
            error = AutoLockRenewTimeout(f"Auto-renew period ({max_lock_renewal_duration} seconds) elapsed.")
            renewable.auto_renew_error = error
            lock_expired = renewable._lock_expired
        except Exception as e:  # pylint: disable=broad-except
            self.broker.logger.warning("Failed to auto-renew lock: %r. Closing thread.", e)
            error = AutoLockRenewFailed("Failed to auto-renew lock", error=e)
            renewable.auto_renew_error = error
        finally:
            if on_lock_renew_failure and lock_expired:
                await on_lock_renew_failure(renewable, error)
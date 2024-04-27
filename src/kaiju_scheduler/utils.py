"""Utility classes and functions."""

import asyncio
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional, Tuple, Type, TypeVar

from kaiju_scheduler.interfaces import Logger

__all__ = ["timeout", "retry", "RetryError"]


_Item = TypeVar("_Item")


class RetryError(Exception):
    """Error recognized by :py:func:`~kaiju_scheduler.utils.retry` as suitable for retry."""


def timeout(time_sec: float, /):
    """Execute async callables within a timeout.

    .. code-block:: python

        async with timeout(5):
            await do_something_asynchronous()

    """
    return _Timeout(time_sec)


class _Timeout:
    __slots__ = ("_timeout", "_loop", "_task", "_handler")

    _handler: asyncio.Handle

    def __init__(self, time_sec: float, loop=None):
        self._timeout = max(0.0, time_sec)
        self._loop = loop
        # self._handler: asyncio.Task = None

    async def __aenter__(self):
        if self._loop is None:
            loop = asyncio.get_running_loop()
        else:
            loop = self._loop
        task = asyncio.current_task()
        self._handler = loop.call_at(loop.time() + self._timeout, self._cancel_task, task)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is asyncio.CancelledError:
            raise asyncio.TimeoutError("Timeout")
        if self._handler:
            self._handler.cancel()

    @staticmethod
    def _cancel_task(task: asyncio.Task):
        task.cancel()


async def retry(
    func: Callable[..., Awaitable[Any]],
    retries: int,
    args: Iterable[Any] = tuple(),
    kws: Mapping[str, Any] = MappingProxyType({}),
    *,
    interval_s: float = 1.0,
    timeout_s: float = 120.0,
    catch_exceptions: Tuple[Type[BaseException], ...] = (TimeoutError, IOError, ConnectionError, RetryError),
    logger: Optional[Logger] = None,
):
    """Retry function call

    :param func: async callable
    :param retries: number of retries
    :param args: positional arguments
    :param kws: keyword arguments
    :param interval_s: interval in seconds between retries
    :param timeout_s: total timeout in seconds for all retries
    :param catch_exceptions: catch certain exception types and retry when they happen
    :param logger: optional logger
    :return: returns the function result
    """
    async with timeout(timeout_s):
        while retries + 1 > 0:
            try:
                return await func(*args, **kws)
            except catch_exceptions as exc:
                retries -= 1
                if logger is not None:
                    logger.info("retrying on error", exc_info=exc)
                await asyncio.sleep(interval_s)

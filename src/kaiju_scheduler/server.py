"""Simple asyncio server implementation."""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Collection, Final, List, Mapping, Optional, Tuple

from kaiju_scheduler.interfaces import Logger
from kaiju_scheduler.utils import retry, timeout

__all__ = ["Server", "Aborted", "ServerClosed"]

_RequestBatch = Collection[Tuple[Callable[..., Awaitable], Mapping[str, Any]]]
_Callback = Optional[Callable[..., Awaitable]]


class Aborted(RuntimeError):
    """The request was aborted by the server."""


class ServerClosed(RuntimeError):
    """The server is closed."""


@dataclass
class Server:
    """Simple asyncio server for internal function calls.

    Provides a mechanism for request limiting, timeouts and retries.
    """

    max_parallel_tasks: int = 256
    """Maximum number of parallel calls."""

    logger: Optional[Logger] = None
    """Optional logger instance."""

    full: bool = field(init=False, default=False)
    """Server is full and cant accept connections."""

    closed: bool = field(init=False, default=True)
    """Server is closed and cannot accept new requests."""

    server_not_full: Final[asyncio.Event] = field(init=False, default_factory=asyncio.Event)
    """The event is set when the server is not full and can accept new requests."""

    server_empty: Final[asyncio.Event] = field(init=False, default_factory=asyncio.Event)
    """The event is set when the server is empty and can now shut down."""

    _request_counter: int = field(init=False, default=0)
    """Current number of requests."""

    def call_nowait(
        self,
        func: Callable[..., Awaitable],
        params: Mapping[str, Any],
        *,
        request_timeout_s: float = 300,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
        task_name: Optional[str] = None,
    ) -> asyncio.Task:
        """Create a new task and return immediately.

        .. note::

            The method is designed so the task doesn't raise errors. It returns them instead in its result and
            passes them to the callback function if it was provided.

        :param func: The function to call
        :param params: The parameters to pass to the function
        :param request_timeout_s: total max request time, the request will return `asyncio.TimeoutError`
            once the timeout is reached
        :param callback: The callback function which will be called with the result
        :param retries: How many times to retry the call
        :param retry_interval_s: How long to wait before retries
        :param task_name: custom asyncio task name

        :returns: an asyncio task wrapper around the request
        :raises asyncio.QueueFull: if the server is full, use :py:attr:`~kaiju_scheduler.server.Server.full` or
            :py:attr:`~kaiju_scheduler.server.Server.server_not_full` event to check before submitting a request
        :raises ServerClosed: if the server is closed and cannot accept a request
        """
        if self.closed:
            raise ServerClosed("Server is closed")
        if self.full:
            raise asyncio.QueueFull("Server is full")
        self._increment_counter()
        return asyncio.create_task(
            self._call(func, params, request_timeout_s, callback, retries, retry_interval_s), name=task_name
        )

    def call_many_nowait(
        self,
        batch: _RequestBatch,
        *,
        request_timeout_s: float = 300,
        abort_batch_on_error: bool = False,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
        task_name: Optional[str] = None,
    ) -> asyncio.Task:
        """Create a new task batch and return immediately.

        When batch will be called its requests will be executed in order which allows request chaining.

        .. note::

            The method is designed so the task doesn't raise errors. It returns them instead in its result and
            passes them to the callback function if it was provided.

        :param batch: batch of request data, i.e. list of (func, params) tuples
        :param request_timeout_s: total max request time for the whole batch, each request in a batch after the timeout
            will return `asyncio.TimeoutError`
        :param abort_batch_on_error: abort the whole batch on a first exception, all subsequent requests in the batch
            will return an :py:class:`~kaiju_scheduler.server.Server.Aborted` exceptions
        :param callback: The callback function which will be called with the result
        :param retries: How many times to retry the call
        :param retry_interval_s: How long to wait before retries
        :param task_name: custom asyncio task name

        :returns: an asyncio task wrapper around the request
        :raises asyncio.QueueFull: if the server is full, use :py:attr:`~kaiju_scheduler.server.Server.full` or
            :py:attr:`~kaiju_scheduler.server.Server.server_not_full` event to check before submitting a request
        :raises ServerClosed: if the server is closed and cannot accept a request
        """
        if self.closed:
            raise ServerClosed("Server is closed")
        if self.full:
            raise asyncio.QueueFull("Server is full")
        self._increment_counter()
        return asyncio.create_task(
            self._call_many(batch, request_timeout_s, abort_batch_on_error, callback, retries, retry_interval_s),
            name=task_name,
        )

    async def call(
        self,
        func: Callable[..., Awaitable],
        params: Mapping[str, Any],
        *,
        request_timeout_s: float = 300,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
        task_name: Optional[str] = None,
    ) -> asyncio.Task:
        """Same as :py:meth:`~kaiju_scheduler.server.Server.call_nowait` but would wait for the server counter
        instead of raising a `asyncio.QueueFull` error."""
        if self.closed:
            raise ServerClosed("Server is closed")
        if self.full:
            await self.server_not_full.wait()
        self._increment_counter()
        return asyncio.create_task(
            self._call(func, params, request_timeout_s, callback, retries, retry_interval_s), name=task_name
        )

    async def call_many(
        self,
        batch: _RequestBatch,
        *,
        request_timeout_s: float = 300,
        abort_batch_on_error: bool = False,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
        task_name: Optional[str] = None,
    ) -> asyncio.Task:
        """Same as :py:meth:`~kaiju_scheduler.server.Server.call_many_nowait` but would wait for the server counter
        instead of raising a `asyncio.QueueFull` error."""
        if self.closed:
            raise ServerClosed("Server is closed")
        if self.full:
            await self.server_not_full.wait()
        self._increment_counter()
        return asyncio.create_task(
            self._call_many(batch, request_timeout_s, abort_batch_on_error, callback, retries, retry_interval_s),
            name=task_name,
        )

    async def _call(
        self,
        func: Callable[..., Awaitable],
        params: Mapping[str, Any],
        request_timeout_s: float = 300,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
    ) -> Any:
        result = None
        try:
            async with timeout(request_timeout_s):
                if retries:
                    result = await retry(
                        func,
                        kws=params,
                        retries=retries,
                        interval_s=retry_interval_s,
                        timeout_s=request_timeout_s,
                        logger=self.logger,
                    )
                else:
                    result = await func(**params)
        except Exception as exc:
            result = exc
        finally:
            self._decrement_counter()
            if callback is not None:
                await callback(result)
        return result

    async def _call_many(
        self,
        batch: _RequestBatch,
        request_timeout_s: float = 300,
        abort_batch_on_error: bool = False,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
    ) -> List[Any]:
        n, results = 0, []
        try:
            async with timeout(request_timeout_s):
                for n, (func, params) in enumerate(batch):
                    try:
                        if retries:
                            result = await retry(
                                func,
                                kws=params,
                                retries=retries,
                                interval_s=retry_interval_s,
                                timeout_s=request_timeout_s,
                                logger=self.logger,
                            )
                        else:
                            result = await func(**params)
                        results.append(result)
                    except Exception as exc:
                        results.append(exc)
                        if abort_batch_on_error:
                            results.extend([Aborted("Request aborted")] * (len(batch) - n - 1))
                            break
        except asyncio.TimeoutError:
            results.extend([asyncio.TimeoutError("Timeout")] * (len(batch) - n))
        except asyncio.CancelledError:
            results.extend([Aborted("Request aborted")] * (len(batch) - n))
        finally:
            self._decrement_counter()
            if callback is not None:
                await callback(results)
        return results

    async def start(self):
        self._reset_counter()
        self.closed = False

    async def stop(self):
        self.closed = True
        await self.server_empty.wait()

    def json_repr(self):
        return {
            "full": self.full,
            "max_parallel_tasks": self.max_parallel_tasks,
            "request_counter": self._request_counter,
        }

    async def __aenter__(self) -> "Server":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    def _increment_counter(self) -> None:
        self._request_counter += 1
        if self._request_counter == self.max_parallel_tasks:
            self.server_not_full.clear()
            self.full = True

    def _decrement_counter(self) -> None:
        self._request_counter -= 1
        if self._request_counter == 0:
            self.server_empty.set()
        if not self.server_not_full.is_set():
            self.server_not_full.set()
            self.full = False

    def _reset_counter(self) -> None:
        self._request_counter = 0
        self.full = False
        self.server_not_full.set()
        self.server_empty.set()

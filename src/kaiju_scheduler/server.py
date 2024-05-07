"""Simple asyncio server implementation."""

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Collection, Iterable, Final, List, Mapping, Optional, Tuple

from template_dict import Template

from kaiju_scheduler.interfaces import Logger
from kaiju_scheduler.utils import retry, timeout

__all__ = ["Server", "Aborted", "ServerClosed"]

_RequestBatch = Collection[Tuple[Callable[..., Awaitable], Iterable[Any], Mapping[str, Any]]]
_Callback = Optional[Callable[..., Awaitable]]
_Context = Optional[Mapping[str, Any]]


class Aborted(RuntimeError):
    """The request was aborted by the server."""


class ServerClosed(RuntimeError):
    """The server is closed."""


@dataclass
class Server:
    """Simple asyncio server for internal function calls.

    Provides a mechanism for request limiting, timeouts and retries.
    """

    context: ContextVar[_Context] = ContextVar("Server", default=None)

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
        args: Iterable[Any] = tuple(),
        kws: Mapping[str, Any] = MappingProxyType({}),
        *,
        request_timeout_s: float = 300,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
        task_name: Optional[str] = None,
        ctx: Optional[_Context] = None,
    ) -> asyncio.Task:
        """Create a new task and return immediately.

        .. note::

            The method is designed so the task doesn't raise errors. It returns them instead in its result and
            passes them to the callback function if it was provided.

        :param func: The function to call
        :param args: Positional arguments
        :param kws: Keyword arguments
        :param request_timeout_s: total max request time, the request will return `asyncio.TimeoutError`
            once the timeout is reached
        :param callback: The callback function which will be called with the result
        :param retries: How many times to retry the call
        :param retry_interval_s: How long to wait before retries
        :param task_name: custom asyncio task name
        :param ctx: context variable with the context for this call

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
            self._call(func, args, kws, request_timeout_s, callback, retries, retry_interval_s, ctx), name=task_name
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
        ctx: _Context = None,
    ) -> asyncio.Task:
        """Create a new task batch and return immediately.

        When batch will be called its requests will be executed in order which allows request chaining.

        .. note::

            The method is designed so the task doesn't raise errors. It returns them instead in its result and
            passes them to the callback function if it was provided.

        :param batch: batch of requests i.e. a collection of (func, args, kws) tuples
        :param request_timeout_s: total max request time for the whole batch, each request in a batch after the timeout
            will return `asyncio.TimeoutError`
        :param abort_batch_on_error: abort the whole batch on a first exception, all subsequent requests in the batch
            will return an :py:class:`~kaiju_scheduler.server.Server.Aborted` exceptions
        :param callback: The callback function which will be called with the result
        :param retries: How many times to retry the call
        :param retry_interval_s: How long to wait before retries
        :param task_name: custom asyncio task name
        :param ctx: context variable with the context for this call

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
            self._call_many(batch, request_timeout_s, abort_batch_on_error, callback, retries, retry_interval_s, ctx),
            name=task_name,
        )

    async def call(
        self,
        func: Callable[..., Awaitable],
        args: Iterable[Any] = tuple(),
        kws: Mapping[str, Any] = MappingProxyType({}),
        *,
        request_timeout_s: float = 300,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
        task_name: Optional[str] = None,
        ctx: _Context = None,
    ) -> asyncio.Task:
        """Same as :py:meth:`~kaiju_scheduler.server.Server.call_nowait` but would wait for the server counter
        instead of raising a `asyncio.QueueFull` error."""
        if self.closed:
            raise ServerClosed("Server is closed")
        if self.full:
            await self.server_not_full.wait()
        self._increment_counter()
        return asyncio.create_task(
            self._call(func, args, kws, request_timeout_s, callback, retries, retry_interval_s, ctx), name=task_name
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
        ctx: _Context = None,
    ) -> asyncio.Task:
        """Same as :py:meth:`~kaiju_scheduler.server.Server.call_many_nowait` but would wait for the server counter
        instead of raising a `asyncio.QueueFull` error."""
        if self.closed:
            raise ServerClosed("Server is closed")
        if self.full:
            await self.server_not_full.wait()
        self._increment_counter()
        return asyncio.create_task(
            self._call_many(batch, request_timeout_s, abort_batch_on_error, callback, retries, retry_interval_s, ctx),
            name=task_name,
        )

    def call_template_nowait(
        self,
        batch: _RequestBatch,
        *,
        request_timeout_s: float = 300,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
        task_name: Optional[str] = None,
        env: dict = None,
        ctx: _Context = None,
    ) -> asyncio.Task:
        """Create a new task batch with templates and return immediately.

        When batch will be called its requests will be executed in order which allows request chaining.

        The difference from :py:meth:`~kaiju_scheduler.server.Server.call_many_nowait` is that this method executes
        requests inside a template engine. This means you can pass results between requests before returning
        the results.

        For example, you have a method what returns a list of users and another which requires a list of user ids
        to send notifications. With the `template syntax <https://template-dict.readthedocs.io/guide.html>`_
        you ask the server to pass the results from the first method to the next one
        using `[result.<request_id>.*]` placeholder. Request ids always start from zero.

        .. code-block:: python

            server.call_template_nowait([
                (get_users, [], {'where': {'blocked': '[env.user_condition]'}}),
                (notify_users, [], {'user_ids': '[result.0.id]'}),
            ], env={'user_condition': {'blocked': True}})

        This method is a bit slower than :py:meth:`~kaiju_scheduler.server.Server.call_many_nowait` for obvious
        reasons. Use the other one if you don't need templates.

        .. note::

            The method is designed so the task doesn't raise errors. It returns them instead in its result and
            passes them to the callback function if it was provided.

        :param batch: batch of requests i.e. a collection of (func, args, kws) tuples
        :param request_timeout_s: total max request time for the whole batch, each request in a batch after the timeout
            will return `asyncio.TimeoutError`
        :param callback: The callback function which will be called with the result
        :param retries: How many times to retry the call
        :param retry_interval_s: How long to wait before retries
        :param task_name: custom asyncio task name
        :param env: shared environment variables between requests, accessible with `[env.*]` template placeholder
        :param ctx: context variable with context for this request

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
        if env is None:
            env = {}
        self._increment_counter()
        return asyncio.create_task(
            self._call_template(batch, request_timeout_s, callback, retries, retry_interval_s, env, ctx),
            name=task_name,
        )

    async def call_template(
        self,
        batch: _RequestBatch,
        *,
        request_timeout_s: float = 300,
        callback: _Callback = None,
        retries: int = 0,
        retry_interval_s: float = 0,
        task_name: Optional[str] = None,
        env: dict = None,
        ctx: _Context = None,
    ) -> asyncio.Task:
        """Same as :py:meth:`~kaiju_scheduler.server.Server.call_template_nowait` but would wait for the server counter
        instead of raising a `asyncio.QueueFull` error."""
        if self.closed:
            raise ServerClosed("Server is closed")
        if self.full:
            await self.server_not_full.wait()
        if env is None:
            env = {}
        self._increment_counter()
        return asyncio.create_task(
            self._call_template(batch, request_timeout_s, callback, retries, retry_interval_s, env, ctx),
            name=task_name,
        )

    async def _call(
        self,
        func: Callable[..., Awaitable],
        args: Iterable[Any],
        kws: Mapping[str, Any],
        request_timeout_s: float,
        callback: _Callback,
        retries: int,
        retry_interval_s: float,
        ctx: _Context,
    ) -> Any:
        if ctx:
            self.context.set(ctx)
        result = None
        try:
            async with timeout(request_timeout_s):
                if retries:
                    result = await retry(
                        func,
                        args=args,
                        kws=kws,
                        retries=retries,
                        interval_s=retry_interval_s,
                        timeout_s=request_timeout_s,
                        logger=self.logger,
                    )
                else:
                    result = await func(*args, **kws)
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
        request_timeout_s,
        abort_batch_on_error: bool,
        callback: _Callback,
        retries: int,
        retry_interval_s: float,
        ctx: _Context,
    ) -> List[Any]:
        if ctx:
            self.context.set(ctx)
        n, results = 0, []
        try:
            async with timeout(request_timeout_s):
                for n, (func, args, kws) in enumerate(batch):
                    try:
                        if retries:
                            result = await retry(
                                func,
                                args=args,
                                kws=kws,
                                retries=retries,
                                interval_s=retry_interval_s,
                                timeout_s=request_timeout_s,
                                logger=self.logger,
                            )
                        else:
                            result = await func(*args, **kws)
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

    async def _call_template(
        self,
        batch: _RequestBatch,
        request_timeout_s: float,
        callback: _Callback,
        retries: int,
        retry_interval_s: float,
        env: dict,
        ctx: _Context,
    ) -> List[Any]:
        if ctx:
            self.context.set(ctx)
        n, results = 0, []
        template_env = {"env": env, "result": {}}
        try:
            async with timeout(request_timeout_s):
                for n, (func, args, kws) in enumerate(batch):
                    try:
                        kws = Template(kws).eval(template_env)
                        if retries:
                            result = await retry(
                                func,
                                args=args,
                                kws=kws,
                                retries=retries,
                                interval_s=retry_interval_s,
                                timeout_s=request_timeout_s,
                                logger=self.logger,
                            )
                        else:
                            result = await func(*args, **kws)
                        results.append(result)
                        template_env["result"][str(n)] = result
                    except Exception as exc:
                        results.append(exc)
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

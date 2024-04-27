"""Scheduler classes."""

import asyncio
import traceback
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Awaitable, Callable, ClassVar, Dict, List, Mapping, Optional, final
from weakref import proxy

from kaiju_scheduler.interfaces import Logger
from kaiju_scheduler.utils import retry, timeout

__all__ = ["ExecPolicy", "ScheduledTask", "Scheduler"]

_AsyncCallable = Callable[..., Awaitable[Any]]
_Sentinel = ...


@final
class ExecPolicy(Enum):
    """Function execution policy for a scheduled task."""

    CANCEL = "CANCEL"
    """Cancel the previous call immediately if it's still going and restart strictly on interval_s."""

    WAIT = "WAIT"
    """Wait for the previous call to finish.

    This policy actually has a safe wait timeout, it's just bigger than its interval_s.
    See :py:attr:`kaiju_scheduler.scheduler.Scheduler.wait_task_timeout_safe_mod`. On scheduler exit this task
    will still be cancelled immediately.
    """

    SHIELD = "SHIELD"
    """Shield the task from cancellation.

    This will not allow the scheduler to cancel the task even on exit. The scheduler will try to wait until
    the task finishes.

    Use this policy wisely because it can lead to stall tasks. Sometimes it may be useful though if it performs some
    sensitive operation.

    Systems such as as Docker or web servers also have their own graceful timeouts.
    This policy doesn't shield the task from being cancelled due to SIGTERM for obvious reasons.
    """


@final
class ScheduledTask:
    """Scheduled task."""

    class _TaskSuspendCtx:
        __slots__ = ("__weakref__", "_task")

        def __init__(self, task: "ScheduledTask", /):
            self._task = proxy(task)

        async def __aenter__(self):
            self._task.disable()
            await self._task.wait()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            self._task.enable()

    __slots__ = (
        "_scheduler",
        "name",
        "method",
        "args",
        "kws",
        "interval_s",
        "retries",
        "retry_interval_s",
        "max_timeout_s",
        "result",
        "_policy",
        "_called_at",
        "_enabled",
        "_idle",
        "_executed_task",
        "__weakref__",
    )

    def __init__(
        self,
        scheduler: "Scheduler",
        name: str,
        method: _AsyncCallable,
        args: tuple,
        kws: Mapping,
        interval_s: float,
        policy: ExecPolicy,
        max_timeout_s: float,
        retries: int,
        retry_interval_s: float,
    ):
        """Initialize."""
        self.name = name
        self.method = method
        self.args = args
        self.kws = kws
        self.interval_s = interval_s
        self.max_timeout_s = max_timeout_s
        self.retries = retries
        self.retry_interval_s = retry_interval_s
        self.result = None
        self._policy = policy
        self._called_at = 0.0
        self._scheduler = proxy(scheduler)
        self._enabled: bool = False
        self._executed_task: Optional[asyncio.Task] = None
        self._idle = asyncio.Event()
        self._idle.set()

    def enable(self) -> None:
        """Enable and schedule next run."""
        self._enabled = True
        if not self._executed_task:
            self.run()

    def disable(self) -> None:
        """Disable the task for future execution.

        This will not cancel the current execution if it's already running.
        """
        self._enabled = False

    def suspend(self) -> _TaskSuspendCtx:
        """Temporarily suspend execution of a task within a context block."""
        return self._TaskSuspendCtx(self)

    async def wait(self) -> None:
        """Wait until the current run has finished."""
        await self._idle.wait()

    def json_repr(self) -> Dict[str, Any]:
        """Get JSON compatible object state info."""
        return {
            "name": self.name,
            "policy": self._policy.value,
            "enabled": self._enabled,
            "started": not self._idle.is_set(),
            "interval_s": self.interval_s,
            "max_timeout_s": self.max_timeout_s,
            "retries": self.retries,
            "retry_interval_s": self.retry_interval_s,
            "called_at": self._called_at,
        }

    def run(self) -> None:
        self._executed_task = self._scheduler.loop.create_task(self._run(), name=self.name)

    def _logger(self, msg: str, /):
        if self._scheduler.logger is not None:
            self._scheduler.logger.info(msg)

    async def _run(self) -> None:
        loop_time = self._scheduler.loop.time()
        sleep_interval_s = max(0, self._called_at + self.interval_s - loop_time)
        await asyncio.sleep(sleep_interval_s)
        self._called_at = loop_time + sleep_interval_s
        if not self._enabled:
            self._executed_task = None
            return

        self._idle.clear()
        try:
            if self.retries:
                self.result = await retry(
                    self.method,
                    args=self.args,
                    kws=self.kws,
                    retries=self.retries,
                    interval_s=self.retry_interval_s,
                    timeout_s=self.max_timeout_s,
                )
            else:
                async with timeout(self.max_timeout_s):
                    self.result = await self.method(*self.args, **self.kws)
        except asyncio.TimeoutError:
            self._logger(f'Task timed out "{self.name}"')
        except Exception:
            self._logger(traceback.format_exc())
        finally:
            self._idle.set()
            self._executed_task = None
            if self._enabled:
                self.run()


@dataclass
class Scheduler:
    """Schedule and execute local asynchronous functions periodically."""

    ExecPolicy: ClassVar = ExecPolicy

    wait_task_timeout_safe_mod: ClassVar[float] = 4.0
    """Timeout modifier for WAIT tasks (to prevent them waiting forever)."""

    logger: Optional[Logger] = None
    """Optional logger instance."""

    loop: asyncio.AbstractEventLoop = None  # type: ignore
    """Asyncio loop to use with the scheduler."""

    tasks: List[ScheduledTask] = field(init=False, default_factory=list)
    """List of registered tasks."""

    _started: bool = field(init=False, default=False)

    def schedule_task(
        self,
        func: Callable,
        interval_s: float,
        args: tuple = tuple(),
        kws: Mapping = MappingProxyType({}),
        *,
        policy: ExecPolicy = ExecPolicy.CANCEL,
        max_timeout_s: float = 0,
        retries: int = 0,
        retry_interval_s: float = 0,
        name: Optional[str] = None,
    ) -> ScheduledTask:
        """Schedule a periodic task.

        :param func: asynchronous function
        :param args: input positional arguments
        :param kws: input kw arguments
        :param interval_s: schedule interval_s in seconds
        :param policy: task execution policy
        :param max_timeout_s: optional max timeout in seconds, for :py:obj:`~kaiju_scheduler.scheduler.ExecPolicy.CANCEL`
            the lowest between `max_timeout_s` and `interval_s` will be used, by default `interval_s` is used for
            cancelled tasks and `interval_s * 4` for waited tasks
            max_timeout_s is ignored for :py:obj:`~kaiju_scheduler.scheduler.ExecPolicy.SHIELD` policies
        :param retries: number of retries if any, see :py:func:`~kaiju_scheduler.utils.retry` for more info
        :param retry_interval_s: interval_s between retries, see :py:func:`~kaiju_scheduler.utils.retry` for more info
        :param name: optional custom task name, which will be shown in the app's server list of task
        :returns: an instance of scheduled task
        """
        if name is None:
            name = f"scheduled:{func.__qualname__}"
        if retries and not retry_interval_s:
            retry_interval_s = interval_s / (retries + 1)
        max_timeout_s = self._create_timeout(policy, interval_s, max_timeout_s)
        if self.logger is not None:
            self.logger.debug(f"Schedule task {name}")
        task = ScheduledTask(self, name, func, args, kws, interval_s, policy, max_timeout_s, retries, retry_interval_s)
        self.tasks.append(task)
        if self._started:
            task.enable()
        return task

    async def start(self):
        """Initialize the loop and enable all tasks."""
        if not self.loop:
            self.loop = asyncio.get_running_loop()
        for task in self.tasks:
            task.enable()
        self._started = True

    async def stop(self):
        """Close and cancel all tasks.

        Note that it will not clear the :py:attr:`~kaiju_scheduler.scheduler.Scheduler.tasks` list.
        """
        self._started = False
        _tasks = []
        for task in self.tasks:
            task.disable()
            executed_task = task._executed_task
            if executed_task and not executed_task.done():
                if task._policy is not ExecPolicy.SHIELD:
                    executed_task.cancel()
                _tasks.append(executed_task)

        if _tasks:
            await asyncio.wait(_tasks)

    def json_repr(self) -> Dict[str, Any]:
        """Get JSON compatible object state info."""
        return {"started": self._started, "time": self.loop.time(), "tasks": [task.json_repr() for task in self.tasks]}

    async def __aenter__(self) -> "Scheduler":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    def _create_timeout(self, policy: ExecPolicy, interval_s: float, max_timeout_s: float, /) -> float:
        if policy is self.ExecPolicy.CANCEL:
            return min(interval_s, max_timeout_s) if max_timeout_s else interval_s
        elif policy is self.ExecPolicy.WAIT:
            return max_timeout_s if max_timeout_s else self.wait_task_timeout_safe_mod * interval_s
        elif policy is self.ExecPolicy.SHIELD:
            return float("Inf")
        else:
            raise ValueError(f"Unsupported policy {policy}")

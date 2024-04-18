[![pypi](https://img.shields.io/pypi/v/kaiju-scheduler.svg)](https://pypi.python.org/pypi/kaiju-scheduler/)
[![docs](https://readthedocs.org/projects/kaiju-scheduler/badge/?version=latest&style=flat)](https://kaiju-scheduler.readthedocs.io)
[![codecov](https://codecov.io/gh/violet-black/kaiju-scheduler/graph/badge.svg?token=FEUUMQELFX)](https://codecov.io/gh/violet-black/kaiju-scheduler)
[![tests](https://github.com/violet-black/kaiju-scheduler/actions/workflows/tests.yaml/badge.svg)](https://github.com/violet-black/kaiju-scheduler/actions/workflows/tests.yaml)
[![mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[![python](https://img.shields.io/pypi/pyversions/kaiju-scheduler.svg)](https://pypi.python.org/pypi/kaiju-scheduler/)

**kaiju-scheduler** is a simple asynchronous tasks scheduler / executor for asyncio functions. It adds a bit of extra
such as retries, timeouts, execution policies etc.

# Installation

With pip and python 3.8+:

```bash
pip3 install kaiju-scheduler
```

# How to use

See the [user guide](https://kaiju-scheduler.readthedocs.io/guide.html) for more info.

Initialize a scheduler and schedule your procedure for periodic execution. Then start the scheduler.

```python
from kaiju_scheduler import Scheduler

async def call_async_procedure(*args, **kws):
    ...

async def main():
    scheduler = Scheduler()
    scheduler.schedule_task(call_async_procedure, interval_s=10, args=(1, 2), kws={'value': True})
    await scheduler.start()
    ...
    await scheduler.stop()

```

Alternatively you can use the scheduler contextually.

```python
async def main():
    async with Scheduler() as scheduler:
        scheduler.schedule_task(call_async_procedure, interval_s=10, args=(1, 2), kws={'value': True})
```

`Scheduler.schedule_task` returns a task object which you can enable / disable or supress the task execution in
your code temporarily using `task.suspend` context.  You can also access the previous call results from `task.result` attribute.

```python

class Cache:

    def __init__(self, scheduler: Scheduler):
        self._scheduler = scheduler
        self._cache_task = self._scheduler.schedule_task(
            self.cache_all, interval_s=600, policy=scheduler.ExecPolicy.WAIT)

    async def cache_all(self):
        ...

    async def reconfigure_cache(self):
        async with self._cache_task.suspend():
            "Do something while the caching is suspended"

```

You can specify retries for common types of errors such as `IOError` or `ConnectionError` using `retries` parameter.
The scheduler will try to retry the call on such type of error.

```python
scheduler.schedule_task(call_async_procedure, interval_s=300, retries=3, retry_interval_s=1)
```

There are various policies considering task execution.
See the [reference](https://kaiju-scheduler.readthedocs.io/reference.html) for more info on that.

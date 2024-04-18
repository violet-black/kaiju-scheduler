import asyncio

import pytest

from kaiju_scheduler.scheduler import Scheduler


class _TestClass:

    def __init__(self, interval: float):
        self.interval = interval
        self.counter = 0
        self.retry_counter = 0

    def run_sync(self, *args, **kws):
        self.counter += 1
        return args, kws

    async def run(self, *args, **kws):
        await asyncio.sleep(self.interval)
        self.counter += 1
        return args, kws

    async def retry(self, retries: int):
        """Retry test call with a certain number of consequent failures."""
        if self.retry_counter < retries:
            self.retry_counter += 1
            raise ConnectionError()
        self.counter += 1
        return retries


@pytest.mark.asyncio
class TestScheduler:

    @pytest.fixture(autouse=True)
    def _loop(self):
        loop = asyncio.get_event_loop()
        loop.set_debug(True)

    async def test_execution(self):
        service = _TestClass(0.001)
        async with Scheduler() as sh:
            sh.schedule_task(service.run, interval_s=0.01)
            await asyncio.sleep(0.02)
            assert service.counter > 0, 'must execute the task'

    async def test_multiple_tasks(self):
        services = [_TestClass(0.001) for _ in range(10)]
        async with Scheduler() as sh:
            for service in services:
                sh.schedule_task(service.run, interval_s=0.01)
            await asyncio.sleep(0.02)

        for service in services:
            assert service.counter > 0, 'must execute the task'

    async def test_policy_cancel(self):
        service = _TestClass(10)
        async with Scheduler() as sh:
            sh.schedule_task(service.run, interval_s=0.01)
            await asyncio.sleep(0.02)
            assert service.counter == 0, 'must cancel the task on timeout'

    async def test_policy_wait(self):
        service = _TestClass(0.02)
        asyncio.get_event_loop().set_debug(True)
        async with Scheduler() as sh:
            sh.schedule_task(service.run, interval_s=0.01, policy=sh.ExecPolicy.WAIT)
            await asyncio.sleep(0.03)
            assert service.counter > 0, 'must wait for the task to finish'

    async def test_policy_shield(self):
        service = _TestClass(0.025)
        async with Scheduler() as sh:
            sh.schedule_task(service.run, interval_s=0.01, policy=sh.ExecPolicy.SHIELD)
            await asyncio.sleep(0.03)
            assert service.counter > 0, 'shielded task should wait for the task to finish'

    async def test_policy_shield_on_exit(self):
        service = _TestClass(0.025)
        async with Scheduler() as sh:
            sh.schedule_task(service.run, interval_s=0.01, policy=sh.ExecPolicy.SHIELD)
            await asyncio.sleep(0.02)
        assert service.counter > 0, 'shielded task should wait for the task to finish'

    async def test_retries(self):
        service = _TestClass(0.001)
        retries = 3
        async with Scheduler() as sh:
            sh.schedule_task(service.retry, args=(retries,), interval_s=0.01, retries=retries)
            await asyncio.sleep(0.02)
            assert service.counter > 0, 'must execute the task after retries'

    async def test_suspend_task(self):
        service = _TestClass(0.01)
        async with Scheduler() as sh:
            task = sh.schedule_task(service.run, interval_s=0.01)
            async with task.suspend():
                await asyncio.sleep(0.02)
                assert service.counter == 0, 'task should not run while inside suspend context'

    def test_task_info(self):
        service = _TestClass(0)
        sh = Scheduler(loop=asyncio.get_event_loop())
        sh.schedule_task(service.run, interval_s=0.01)
        print(sh.json_repr())

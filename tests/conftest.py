import asyncio
import pytest


def pytest_configure():
    pytest.TestClass = TestClass


class TestClass:

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

    async def sum(self, a, b):
        await asyncio.sleep(self.interval)
        return a + b

    async def fail(self):
        raise ValueError('Failed!')

    async def retry(self, retries: int):
        """Retry test call with a certain number of consequent failures."""
        if self.retry_counter < retries:
            self.retry_counter += 1
            raise ConnectionError()
        self.counter += 1
        return retries

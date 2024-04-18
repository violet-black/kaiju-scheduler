"""Interface types."""

from typing import Protocol

__all__ = ["Logger"]


class Logger(Protocol):

    def error(self, msg, *args, **kwargs) -> None: ...

    def info(self, msg, *args, **kwargs) -> None: ...

    def debug(self, msg, *args, **kwargs) -> None: ...

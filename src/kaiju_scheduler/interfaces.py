"""Interface types."""

from typing import Protocol

__all__ = ["Logger"]


class Logger(Protocol):

    def error(self, msg: str, /, *args, **kwargs) -> None: ...

    def info(self, msg: str, /, *args, **kwargs) -> None: ...

    def debug(self, msg: str, /, *args, **kwargs) -> None: ...

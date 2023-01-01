from __future__ import annotations

from abc import ABC
from contextlib import suppress
from typing import Any, Generic, List, TypeVar

__all__ = 'EventListener', 'EventEmitter'


class EventListener(ABC):
    ...


_T = TypeVar('_T', bound=EventListener)


class EventEmitter(Generic[_T]):
    def __init__(self) -> None:
        super().__init__()
        self._listeners: List[_T] = []

    def add_listener(self, listener: _T) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener: _T) -> None:
        with suppress(ValueError):
            self._listeners.remove(listener)

    async def _emit(self, name: str, *args: Any, **kwds: Any) -> None:
        for listener in self._listeners:
            coro = getattr(listener, 'on_' + name)
            await coro(*args, **kwds)

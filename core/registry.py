from __future__ import annotations

from typing import Any, Callable

from core.models import Job

HandlerFunc = Callable[[Job], dict[str, Any] | None]


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFunc] = {}

    def register(self, job_type: str) -> Callable[[HandlerFunc], HandlerFunc]:
        def decorator(func: HandlerFunc) -> HandlerFunc:
            self._handlers[job_type] = func
            return func

        return decorator

    def get(self, job_type: str) -> HandlerFunc | None:
        return self._handlers.get(job_type)

    def known_types(self) -> list[str]:
        return list(self._handlers.keys())


registry = HandlerRegistry()

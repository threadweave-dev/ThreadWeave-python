from __future__ import annotations

from typing import Any

from threadweave._internal.app import BaseThreadWeave


class SyncExecutor:
    """Execute tasks registered by a synchronous ThreadWeave application."""

    def __init__(self, application: BaseThreadWeave[Any]) -> None:
        self.application = application

    def execute(
        self,
        task_id: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> Any:
        """Resolve and execute a task in the current process."""
        task = self.application.get_task(task_id)
        return task.function(*args, **(kwargs or {}))

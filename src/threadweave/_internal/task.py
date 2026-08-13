from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar

if TYPE_CHECKING:
    from threadweave._internal.app import BaseThreadWeave


P = ParamSpec("P")
R = TypeVar("R")


class TaskOptions:
    """
    Execution options associated with a ThreadWeave Task.
    """

    def __init__(
        self,
        *,
        queue: str | None = None,
        resources: dict[str, Any] | None = None,
        capabilities: tuple[str, ...] = (),
        retries: int = 0,
        timeout: float | None = None,
    ) -> None:
        self.queue = queue
        self.resources = dict(resources or {})
        self.capabilities = tuple(capabilities)
        self.retries = retries
        self.timeout = timeout


class BaseTask(Generic[P, R]):
    """
    Common representation of a ThreadWeave Task.

    BaseTask contains the behavior shared by synchronous and asynchronous
    Task implementations.

    It is intentionally unaware of how jobs are submitted to the
    ThreadWeave Core.
    """

    def __init__(
        self,
        *,
        id: str,
        name: str,
        application: BaseThreadWeave[Any],
        function: Callable[P, R],
        options: TaskOptions,
    ) -> None:
        self._id = id
        self._name = name
        self._application = application
        self._function = function
        self._options = options

    @property
    def id(self) -> str:
        """Return the canonical Task identifier."""
        return self._id

    @property
    def name(self) -> str:
        """Return the local Task name."""
        return self._name

    @property
    def application(self) -> BaseThreadWeave[Any]:
        """Return the application containing this Task."""
        return self._application

    @property
    def function(self) -> Callable[P, R]:
        """Return the underlying Python function."""
        return self._function

    @property
    def options(self) -> TaskOptions:
        """Return the execution options associated with this Task."""
        return self._options

    @property
    def qualified_name(self) -> str:
        """Return the canonical Task identifier."""
        return self._id

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"id={self._id!r}, "
            f"name={self._name!r}"
            f")"
        )

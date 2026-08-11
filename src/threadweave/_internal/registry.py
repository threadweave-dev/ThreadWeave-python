# src/threadweave/runtime/registry.py

from __future__ import annotations

from collections.abc import Iterator, Mapping
from threading import RLock
from types import MappingProxyType
from typing import Protocol, TypeVar, runtime_checkable


class RegistryError(Exception):
    """Base exception raised by the runtime task registry."""


class InvalidTaskIdentifierError(RegistryError, ValueError):
    """Raised when a task identifier is invalid."""


class TaskAlreadyRegisteredError(RegistryError):
    """Raised when another task already uses the same identifier."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(f"Task {task_id!r} is already registered.")


class TaskNotRegisteredError(RegistryError, LookupError):
    """Raised when a task cannot be found in the registry."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(f"Task {task_id!r} is not registered.")


@runtime_checkable
class RegisteredTask(Protocol):
    """
    Minimal interface required by TaskRegistry.

    The concrete Python Task implementation may expose additional attributes
    such as its callable, schemas, resources, retry policy or timeout policy.
    """

    @property
    def id(self) -> str:
        """Return the canonical task identifier."""
        ...


TaskT = TypeVar("TaskT", bound=RegisteredTask)


class TaskRegistry(Mapping[str, TaskT]):
    """
    In-memory registry of Tasks exposed by a Python runtime Application.

    The registry owns no execution or orchestration responsibility. It only
    maps canonical task identifiers to their local Python Task definitions.

    Registration is thread-safe so that discovery mechanisms may register
    Tasks concurrently during application startup.
    """

    def __init__(self) -> None:
        self._tasks: dict[str, TaskT] = {}
        self._lock = RLock()

    def register(self, task: TaskT, *, replace: bool = False) -> TaskT:
        """
        Register a Task.

        Args:
            task:
                Task definition to register.
            replace:
                Replace an existing Task with the same identifier. This should
                normally only be enabled by development reload mechanisms.

        Returns:
            The registered Task, allowing registration to be used naturally
            by decorators.

        Raises:
            TypeError:
                If the object does not expose the required Task interface.
            InvalidTaskIdentifierError:
                If the Task identifier is invalid.
            TaskAlreadyRegisteredError:
                If the identifier is already registered and replacement was
                not explicitly allowed.
        """
        if not isinstance(task, RegisteredTask):
            raise TypeError(
                "Registered objects must expose a string 'id' property."
            )

        task_id = self._validate_task_id(task.id)

        with self._lock:
            existing = self._tasks.get(task_id)

            if existing is not None and existing is not task and not replace:
                raise TaskAlreadyRegisteredError(task_id)

            self._tasks[task_id] = task

        return task

    def unregister(self, task_id: str) -> TaskT:
        """
        Remove and return a registered Task.

        Raises:
            InvalidTaskIdentifierError:
                If the identifier is invalid.
            TaskNotRegisteredError:
                If no Task uses the identifier.
        """
        task_id = self._validate_task_id(task_id)

        with self._lock:
            try:
                return self._tasks.pop(task_id)
            except KeyError:
                raise TaskNotRegisteredError(task_id) from None

    def get_task(self, task_id: str) -> TaskT | None:
        """
        Return a Task by identifier, or None when it is not registered.

        This explicit method avoids ambiguity with Mapping.get() in runtime
        code while retaining the normal Mapping API.
        """
        task_id = self._validate_task_id(task_id)

        with self._lock:
            return self._tasks.get(task_id)

    def require(self, task_id: str) -> TaskT:
        """
        Return a Task by identifier.

        Raises:
            TaskNotRegisteredError:
                If no Task uses the identifier.
        """
        task = self.get_task(task_id)

        if task is None:
            raise TaskNotRegisteredError(task_id)

        return task

    def contains(self, task_id: str) -> bool:
        """Return whether a Task identifier is registered."""
        task_id = self._validate_task_id(task_id)

        with self._lock:
            return task_id in self._tasks

    def snapshot(self) -> Mapping[str, TaskT]:
        """
        Return an immutable snapshot of the current registry.

        The returned mapping is detached from subsequent registry changes and
        can safely be used for task discovery or protocol serialization.
        """
        with self._lock:
            return MappingProxyType(dict(self._tasks))

    def clear(self) -> None:
        """
        Remove all registered Tasks.

        Primarily useful for tests and development reload mechanisms.
        """
        with self._lock:
            self._tasks.clear()

    def __getitem__(self, task_id: str) -> TaskT:
        return self.require(task_id)

    def __iter__(self) -> Iterator[str]:
        # Iterating over a snapshot avoids holding the lock while consumer
        # code executes.
        return iter(self.snapshot())

    def __len__(self) -> int:
        with self._lock:
            return len(self._tasks)

    def __contains__(self, task_id: object) -> bool:
        if not isinstance(task_id, str):
            return False

        try:
            return self.contains(task_id)
        except InvalidTaskIdentifierError:
            return False

    def __repr__(self) -> str:
        with self._lock:
            task_ids = ", ".join(repr(task_id) for task_id in self._tasks)

        return f"{type(self).__name__}([{task_ids}])"

    @staticmethod
    def _validate_task_id(task_id: str) -> str:
        if not isinstance(task_id, str):
            raise InvalidTaskIdentifierError(
                "A Task identifier must be a string."
            )

        if not task_id:
            raise InvalidTaskIdentifierError(
                "A Task identifier cannot be empty."
            )

        if task_id != task_id.strip():
            raise InvalidTaskIdentifierError(
                "A Task identifier cannot contain leading or trailing whitespace."
            )

        if any(character.isspace() for character in task_id):
            raise InvalidTaskIdentifierError(
                f"Task identifier {task_id!r} cannot contain whitespace."
            )

        return task_id

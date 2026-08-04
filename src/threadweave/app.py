from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Iterator, Mapping
from typing import Any, ParamSpec, TypeVar, overload

from threadweave.protocol.client import ProtocolClient
from threadweave.runtime.registry import TaskRegistry
from threadweave.task import Task, TaskOptions

P = ParamSpec("P")
R = TypeVar("R")


_APPLICATION_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")
_NAMESPACE_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.-]*$")


class ThreadWeave:
    """
    Developer-facing entry point for a ThreadWeave Python application.

    A ThreadWeave application:

    - groups and registers Python tasks;
    - defines application-level execution defaults;
    - validates task declarations;
    - provides access to the ThreadWeave Core through a protocol client.

    It does not schedule jobs or execute user code directly.
    """

    def __init__(
        self,
        name: str,
        *,
        namespace: str = "default",
        endpoint: str | None = None,
        default_queue: str | None = None,
        default_resources: Mapping[str, Any] | None = None,
        default_capabilities: tuple[str, ...] = (),
        client: ProtocolClient | None = None,
    ) -> None:
        self._name = self._validate_application_name(name)
        self._namespace = self._validate_namespace(namespace)

        self._default_queue = default_queue
        self._default_resources = dict(default_resources or {})
        self._default_capabilities = tuple(default_capabilities)

        self._registry = TaskRegistry()

        self._client = client or ProtocolClient(
            endpoint=endpoint,
            namespace=self._namespace,
            application=self._name,
        )

    @property
    def name(self) -> str:
        """Return the application name."""
        return self._name

    @property
    def namespace(self) -> str:
        """Return the namespace containing the application."""
        return self._namespace

    @property
    def qualified_name(self) -> str:
        """Return the canonical namespace/application identifier."""
        return f"{self._namespace}/{self._name}"

    @property
    def client(self) -> ProtocolClient:
        """Return the protocol client used to communicate with the Core."""
        return self._client

    @property
    def registry(self) -> TaskRegistry:
        """Return the local task registry."""
        return self._registry

    @overload
    def task(
        self,
        function: Callable[P, R],
        /,
    ) -> Task[P, R]:
        ...

    @overload
    def task(
        self,
        function: None = None,
        /,
        *,
        name: str | None = None,
        queue: str | None = None,
        resources: Mapping[str, Any] | None = None,
        capabilities: tuple[str, ...] | list[str] | None = None,
        retries: int = 0,
        timeout: float | None = None,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        ...

    def task(
        self,
        function: Callable[P, R] | None = None,
        /,
        *,
        name: str | None = None,
        queue: str | None = None,
        resources: Mapping[str, Any] | None = None,
        capabilities: tuple[str, ...] | list[str] | None = None,
        retries: int = 0,
        timeout: float | None = None,
    ) -> Task[P, R] | Callable[[Callable[P, R]], Task[P, R]]:
        """
        Register a function as a ThreadWeave Task.

        The decorator may be used with or without arguments.

        Examples
        --------
        Without options:

            @app.task
            async def resize_image(path: str) -> str:
                ...

        With options:

            @app.task(
                resources={"cpu": 2, "memory": "4Gi"},
                capabilities=["pillow"],
                retries=3,
                timeout=300,
            )
            async def resize_image(path: str) -> str:
                ...
        """

        def decorator(target: Callable[P, R]) -> Task[P, R]:
            return self._register_task(
                target,
                name=name,
                queue=queue,
                resources=resources,
                capabilities=capabilities,
                retries=retries,
                timeout=timeout,
            )

        if function is None:
            return decorator

        if not callable(function):
            raise TypeError(
                "ThreadWeave.task expects a callable or must be used "
                "as a decorator."
            )

        return decorator(function)

    def get_task(self, name: str) -> Task[Any, Any]:
        """
        Return a registered Task by local or fully qualified name.

        Raises
        ------
        KeyError
            If no Task is registered with the requested name.
        """
        return self._registry.get(name)

    def iter_tasks(self) -> Iterator[Task[Any, Any]]:
        """Iterate over all Tasks registered by this application."""
        return iter(self._registry)

    def discover_tasks(self) -> tuple[Task[Any, Any], ...]:
        """
        Return an immutable snapshot of registered Tasks.

        This method is intended for runtime discovery and worker startup.
        """
        return tuple(self._registry)

    async def connect(self) -> None:
        """Open the connection to the ThreadWeave Core."""
        await self._client.connect()

    async def close(self) -> None:
        """Close the connection to the ThreadWeave Core."""
        await self._client.close()

    async def __aenter__(self) -> ThreadWeave:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        await self.close()

    def _register_task(
        self,
        function: Callable[P, R],
        *,
        name: str | None,
        queue: str | None,
        resources: Mapping[str, Any] | None,
        capabilities: tuple[str, ...] | list[str] | None,
        retries: int,
        timeout: float | None,
    ) -> Task[P, R]:
        self._validate_function(function)
        self._validate_execution_options(
            retries=retries,
            timeout=timeout,
        )

        local_name = name or function.__name__
        task_id = self._build_task_id(local_name)

        merged_resources = {
            **self._default_resources,
            **dict(resources or {}),
        }

        merged_capabilities = tuple(
            dict.fromkeys(
                (
                    *self._default_capabilities,
                    *(capabilities or ()),
                )
            )
        )

        options = TaskOptions(
            queue=queue if queue is not None else self._default_queue,
            resources=merged_resources,
            capabilities=merged_capabilities,
            retries=retries,
            timeout=timeout,
        )

        registered_task = Task(
            id=task_id,
            name=local_name,
            application=self,
            function=function,
            options=options,
        )

        self._registry.register(registered_task)

        return registered_task

    def _build_task_id(self, task_name: str) -> str:
        task_name = task_name.strip()

        if not task_name:
            raise ValueError("Task name cannot be empty.")

        if task_name.startswith(".") or task_name.endswith("."):
            raise ValueError(
                f"Invalid task name {task_name!r}: "
                "a task name cannot start or end with a dot."
            )

        return f"{self._namespace}.{self._name}.{task_name}"

    @staticmethod
    def _validate_function(function: Callable[..., Any]) -> None:
        if not inspect.isfunction(function):
            raise TypeError(
                "A ThreadWeave Task must decorate a Python function."
            )

        if inspect.isgeneratorfunction(function):
            raise TypeError(
                "Generator functions are not supported as ThreadWeave Tasks."
            )

        if inspect.isasyncgenfunction(function):
            raise TypeError(
                "Async generator functions are not supported as "
                "ThreadWeave Tasks."
            )

    @staticmethod
    def _validate_execution_options(
        *,
        retries: int,
        timeout: float | None,
    ) -> None:
        if isinstance(retries, bool) or not isinstance(retries, int):
            raise TypeError("retries must be an integer.")

        if retries < 0:
            raise ValueError("retries cannot be negative.")

        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(
                timeout,
                (int, float),
            ):
                raise TypeError("timeout must be a number of seconds.")

            if timeout <= 0:
                raise ValueError("timeout must be greater than zero.")

    @staticmethod
    def _validate_application_name(name: str) -> str:
        name = name.strip()

        if not name:
            raise ValueError("Application name cannot be empty.")

        if not _APPLICATION_NAME_PATTERN.fullmatch(name):
            raise ValueError(
                "Application name must start with a letter and contain "
                "only letters, digits, underscores, or hyphens."
            )

        return name

    @staticmethod
    def _validate_namespace(namespace: str) -> str:
        namespace = namespace.strip()

        if not namespace:
            raise ValueError("Namespace cannot be empty.")

        if not _NAMESPACE_PATTERN.fullmatch(namespace):
            raise ValueError(
                "Namespace must start with a letter and contain only "
                "letters, digits, dots, underscores, or hyphens."
            )

        return namespace

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"name={self._name!r}, "
            f"namespace={self._namespace!r}, "
            f"tasks={len(self._registry)}"
            f")"
        )
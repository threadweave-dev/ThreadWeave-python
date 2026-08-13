from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, ParamSpec, TypeVar, overload

from threadweave._internal.app import BaseThreadWeave
from threadweave._internal.task import TaskOptions
from threadweave.protocol.client import ProtocolClient
from threadweave.task import Task

P = ParamSpec("P")
R = TypeVar("R")


class ThreadWeave(BaseThreadWeave[Task[Any, Any]]):
    """
    Synchronous developer-facing entry point for a ThreadWeave application.

    `ThreadWeave` provides the synchronous Python API for task registration
    and communication with the ThreadWeave Core.

    Application metadata, task registry management, validation, and other
    behavior shared with the asyncio implementation are inherited from
    `BaseThreadWeave`.

    This class is responsible only for behavior specific to the synchronous
    API, including:

    - creation of synchronous `Task` objects;
    - access to the synchronous protocol client;
    - synchronous connection lifecycle management.

    Applications using asyncio should instead import::

        from threadweave.asyncio import ThreadWeave
    """

    def __init__(
        self,
        name: str,
        *,
        namespace: str = "default",
        grpc_address: str | None = None,
        default_queue: str | None = None,
        default_resources: Mapping[str, Any] | None = None,
        default_capabilities: tuple[str, ...] = (),
        client: ProtocolClient | None = None,
    ) -> None:
        super().__init__(
            name,
            namespace=namespace,
            default_queue=default_queue,
            default_resources=default_resources,
            default_capabilities=default_capabilities,
        )

        self._client = client or ProtocolClient(
            endpoint=grpc_address,
            namespace=self._namespace,
            application=self._name,
        )

    @property
    def client(self) -> ProtocolClient:
        """
        Return the synchronous protocol client used to communicate
        with the ThreadWeave Core.
        """
        return self._client

    @overload
    def task(
        self,
        function: Callable[P, R],
        /,
    ) -> Task[P, R]: ...

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
    ) -> Callable[[Callable[P, R]], Task[P, R]]: ...

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
        Register a synchronous Python function as a ThreadWeave Task.

        The decorator may be used with or without arguments.

        Examples
        --------
        Without options::

            @app.task
            def resize_image(path: str) -> str:
                ...

        With options::

            @app.task(
                resources={"cpu": 2, "memory": "4Gi"},
                capabilities=["pillow"],
                retries=3,
                timeout=300,
            )
            def resize_image(path: str) -> str:
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
                "ThreadWeave.task expects a callable or must be used as a decorator."
            )

        return decorator(function)

    def connect(self) -> None:
        """
        Open the synchronous connection to the ThreadWeave Core.
        """
        self._client.connect()

    def close(self) -> None:
        """
        Close the synchronous connection to the ThreadWeave Core.
        """
        self._client.close()

    def __enter__(self) -> ThreadWeave:
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        self.close()

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
        """
        Create and register a synchronous ThreadWeave Task.
        """
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

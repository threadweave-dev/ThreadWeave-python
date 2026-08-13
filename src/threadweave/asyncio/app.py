from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, ParamSpec, TypeVar, overload

from threadweave._internal.app import BaseThreadWeave
from threadweave._internal.task import TaskOptions
from threadweave.asyncio.task import Task
from threadweave.protocol.asyncio.client import AsyncProtocolClient

P = ParamSpec("P")
R = TypeVar("R")


class ThreadWeave(BaseThreadWeave[Task[Any, Any]]):
    """
    Asynchronous developer-facing entry point for a ThreadWeave application.

    `ThreadWeave` provides the asyncio-compatible Python API for task
    registration and communication with the ThreadWeave Core.

    Application metadata, task registry management, validation, and other
    behavior shared with the synchronous implementation are inherited from
    `BaseThreadWeave`.

    This class is responsible only for behavior specific to the asynchronous
    API, including:

    - creation of asynchronous `Task` objects;
    - access to the asynchronous protocol client;
    - asynchronous connection lifecycle management.

    Applications using the synchronous API should instead import::

        from threadweave import ThreadWeave
    """

    def __init__(
        self,
        name: str,
        *,
        namespace: str = "default",
        grpc_address: str | None = None,
        endpoint: str | None = None,
        default_queue: str | None = None,
        default_resources: Mapping[str, Any] | None = None,
        default_capabilities: tuple[str, ...] = (),
        client: AsyncProtocolClient | None = None,
    ) -> None:
        super().__init__(
            name,
            namespace=namespace,
            default_queue=default_queue,
            default_resources=default_resources,
            default_capabilities=default_capabilities,
        )

        if grpc_address is not None and endpoint is not None:
            raise ValueError("grpc_address and endpoint are mutually exclusive")

        self._client = client or AsyncProtocolClient(
            endpoint=grpc_address or endpoint,
            namespace=self._namespace,
            application=self._name,
        )

    @property
    def client(self) -> AsyncProtocolClient:
        """
        Return the asynchronous protocol client used to communicate
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
        Register a Python function as an asynchronous ThreadWeave Task.

        The decorator may be used with or without arguments.

        Examples
        --------
        Without options::

            @app.task
            async def resize_image(path: str) -> str:
                ...

        With options::

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
                "ThreadWeave.task expects a callable or must be used " "as a decorator."
            )

        return decorator(function)

    async def connect(self) -> None:
        """
        Open the asynchronous connection to the ThreadWeave Core.
        """
        await self._client.connect()

    async def close(self) -> None:
        """
        Close the asynchronous connection to the ThreadWeave Core.
        """
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
        """
        Create and register an asynchronous ThreadWeave Task.
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

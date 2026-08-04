from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass, field, replace
from functools import update_wrapper
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    cast,
    overload,
)

from threadweave.job import Job

if TYPE_CHECKING:
    from threadweave.app import ThreadWeave


P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True, slots=True)
class TaskOptions:
    """
    Default execution options attached to a Task definition.

    These options describe the Task's execution requirements and policies.
    They do not determine where or when the Job will execute; that remains
    the responsibility of the ThreadWeave Core.
    """

    queue: str | None = None
    resources: Mapping[str, Any] = field(default_factory=dict)
    capabilities: tuple[str, ...] = ()
    retries: int = 0
    timeout: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "resources", dict(self.resources))
        object.__setattr__(self, "capabilities", tuple(self.capabilities))

        if self.queue is not None:
            queue = self.queue.strip()

            if not queue:
                raise ValueError("queue cannot be empty.")

            object.__setattr__(self, "queue", queue)

        if isinstance(self.retries, bool) or not isinstance(self.retries, int):
            raise TypeError("retries must be an integer.")

        if self.retries < 0:
            raise ValueError("retries cannot be negative.")

        if self.timeout is not None:
            if isinstance(self.timeout, bool) or not isinstance(
                self.timeout,
                (int, float),
            ):
                raise TypeError("timeout must be a number of seconds.")

            if self.timeout <= 0:
                raise ValueError("timeout must be greater than zero.")

            object.__setattr__(self, "timeout", float(self.timeout))

        for capability in self.capabilities:
            if not isinstance(capability, str):
                raise TypeError("capabilities must contain only strings.")

            if not capability.strip():
                raise ValueError("capabilities cannot contain empty values.")

    def with_overrides(
        self,
        *,
        queue: str | None = None,
        resources: Mapping[str, Any] | None = None,
        capabilities: tuple[str, ...] | list[str] | None = None,
        retries: int | None = None,
        timeout: float | None = None,
    ) -> TaskOptions:
        """
        Return a copy with submission-specific overrides.

        Resources are merged with the Task defaults. Capabilities are also
        merged and deduplicated while preserving declaration order.
        """

        merged_resources = {
            **self.resources,
            **dict(resources or {}),
        }

        merged_capabilities = tuple(
            dict.fromkeys(
                (
                    *self.capabilities,
                    *(capabilities or ()),
                )
            )
        )

        return replace(
            self,
            queue=queue if queue is not None else self.queue,
            resources=merged_resources,
            capabilities=merged_capabilities,
            retries=retries if retries is not None else self.retries,
            timeout=timeout if timeout is not None else self.timeout,
        )

    def to_payload(self) -> dict[str, Any]:
        """Serialize the options into a protocol-compatible dictionary."""
        return {
            "queue": self.queue,
            "resources": dict(self.resources),
            "capabilities": list(self.capabilities),
            "retries": self.retries,
            "timeout": self.timeout,
        }

class Task(Generic[P, R]):
    """
    Registered ThreadWeave Task.

    A Task is a reusable definition of work. Calling the object invokes the
    underlying Python function locally. Calling ``submit`` creates a Job
    through the ThreadWeave Core.

    Parameters
    ----------
    id:
        Canonical Task identifier, for example
        ``production.documents.extract_text``.
    name:
        Local Task name within the Application.
    application:
        Application that owns the Task.
    function:
        Python function executed by a compatible worker.
    options:
        Default execution options.
    """

    def __init__(
        self,
        *,
        id: str,
        name: str,
        application: ThreadWeave,
        function: Callable[P, R],
        options: TaskOptions,
    ) -> None:
        if not id:
            raise ValueError("Task id cannot be empty.")

        if not name:
            raise ValueError("Task name cannot be empty.")

        if not callable(function):
            raise TypeError("Task function must be callable.")

        self._id = id
        self._name = name
        self._application = application
        self._function = function
        self._options = options
        self._signature = inspect.signature(function)
        self._is_async = inspect.iscoroutinefunction(function)

        # Make the Task object look like the decorated function to tools such
        # as inspect, IDEs, documentation generators, and test frameworks.
        update_wrapper(self, function)

        # update_wrapper cannot reliably expose this through all wrappers,
        # so it is assigned explicitly.
        self.__signature__ = self._signature

    @property
    def id(self) -> str:
        """Return the canonical Task identifier."""
        return self._id

    @property
    def name(self) -> str:
        """Return the local Task name."""
        return self._name

    @property
    def application(self) -> ThreadWeave:
        """Return the owning ThreadWeave Application."""
        return self._application

    @property
    def function(self) -> Callable[P, R]:
        """
        Return the original decorated function.

        This property is mainly intended for runtime internals, testing, and
        advanced integrations. Normal callers should invoke the Task directly.
        """
        return self._function

    @property
    def options(self) -> TaskOptions:
        """Return the Task's default execution options."""
        return self._options

    @property
    def signature(self) -> inspect.Signature:
        """Return the Python call signature of the decorated function."""
        return self._signature

    @property
    def is_async(self) -> bool:
        """Return whether the underlying function is asynchronous."""
        return self._is_async

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Invoke the underlying function locally.

        This does not create a Job and does not communicate with the Core.

        For an async function, the returned value is the function's coroutine
        object and must therefore be awaited by the caller.
        """
        return self._function(*args, **kwargs)

    def submit(
        self,
        *args: P.args,
        queue: str | None = None,
        resources: Mapping[str, Any] | None = None,
        capabilities: tuple[str, ...] | list[str] | None = None,
        retries: int | None = None,
        timeout: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        **kwargs: P.kwargs,
    ) -> Job[R]:
        """
        Submit this Task synchronously and return its Job handle.

        Use ``asubmit`` from asynchronous code so protocol I/O never blocks
        the running event loop.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.asubmit(
                    *args,
                    queue=queue,
                    resources=resources,
                    capabilities=capabilities,
                    retries=retries,
                    timeout=timeout,
                    metadata=metadata,
                    **kwargs,
                )
            )

        raise RuntimeError(
            "Task.submit() cannot be called from a running event loop; "
            "use 'await task.asubmit(...)' instead."
        )

    async def asubmit(
        self,
        *args: P.args,
        queue: str | None = None,
        resources: Mapping[str, Any] | None = None,
        capabilities: tuple[str, ...] | list[str] | None = None,
        retries: int | None = None,
        timeout: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        **kwargs: P.kwargs,
    ) -> Job[R]:
        """
        Submit this Task asynchronously and return its Job handle.

        Parameters
        ----------
        args, kwargs:
            Arguments passed to the Task when executed by a worker.
        queue:
            Optional queue override.
        resources:
            Additional or overridden resource requirements.
        capabilities:
            Additional required worker capabilities.
        retries:
            Retry-policy override.
        timeout:
            Execution timeout override, expressed in seconds.
        metadata:
            User-defined Job metadata.

        Notes
        -----
        This method is asynchronous because submission involves I/O with the
        ThreadWeave Core.
        """

        self._validate_call(args, kwargs)

        submission_options = self._options.with_overrides(
            queue=queue,
            resources=resources,
            capabilities=capabilities,
            retries=retries,
            timeout=timeout,
        )

        response = await self._application.client.submit_job(
            task_id=self._id,
            args=args,
            kwargs=kwargs,
            options=submission_options.to_payload(),
            metadata=dict(metadata or {}),
        )

        return Job(
            id=response.job_id,
            task_id=self._id,
            client=self._application.client,
        )

    async def _execute(
        self,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> R:
        """
        Execute the Task from the Python worker runtime.

        This is an internal runtime API. It normalizes synchronous and
        asynchronous functions into one awaitable execution interface.
        """

        self._validate_call(args, kwargs)

        result = self._function(*args, **dict(kwargs))

        if inspect.isawaitable(result):
            return await cast(Coroutine[Any, Any, R], result)

        return cast(R, result)

    def _validate_call(
        self,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> None:
        """
        Validate arguments against the original Python signature.

        Type annotations are not checked at runtime. This only verifies the
        structural Python call contract: missing arguments, unexpected
        keyword arguments, duplicate values, and similar invocation errors.
        """
        self._signature.bind(*args, **kwargs)

    def __repr__(self) -> str:
        function_type = "async" if self._is_async else "sync"

        return (
            f"{type(self).__name__}("
            f"id={self._id!r}, "
            f"type={function_type!r}"
            f")"
        )

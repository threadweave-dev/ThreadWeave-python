from __future__ import annotations

from typing import Generic, ParamSpec, TypeVar

from threadweave._internal.task import BaseTask
from threadweave.asyncio.job import Job

P = ParamSpec("P")
R = TypeVar("R")


class Task(BaseTask[P, R], Generic[P, R]):
    """
    Asynchronous ThreadWeave Task.

    `Task` is the concrete task type exposed by the asyncio ThreadWeave API.

    Calling the task directly executes the underlying Python coroutine locally.
    Calling `submit` asynchronously creates a Job through the ThreadWeave Core.

    Notes
    -----
    Applications using this Task implementation are created with::

        from threadweave.asyncio import ThreadWeave

    The synchronous API provides its own Task implementation through the
    top-level ``threadweave`` package.
    """

    async def __call__(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """
        Execute the underlying Python coroutine locally.

        This does not submit a Job to the ThreadWeave Core.
        """
        ...

    async def submit(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Job[R]:
        """
        Submit the Task for execution through the ThreadWeave Core.

        Parameters are forwarded to the underlying task invocation.

        Returns
        -------
        Job[R]
            An asynchronous Job handle representing the submitted execution.
        """
        ...

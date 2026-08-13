from __future__ import annotations

from typing import Generic, ParamSpec, TypeVar

from threadweave._internal.task import BaseTask
from threadweave.job import Job

P = ParamSpec("P")
R = TypeVar("R")


class Task(BaseTask[P, R], Generic[P, R]):
    """
    Synchronous ThreadWeave Task.

    `Task` is the concrete task type exposed by the synchronous ThreadWeave
    API.

    Calling the task directly executes the underlying Python function locally.
    Calling `submit` creates a Job through the ThreadWeave Core.

    Notes
    -----
    Applications using this Task implementation are created with::

        from threadweave import ThreadWeave

    The asyncio API provides its own Task implementation through
    ``threadweave.asyncio``.
    """

    def __call__(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """
        Execute the underlying Python function locally.

        This does not submit a Job to the ThreadWeave Core.
        """
        return self._function(*args, **kwargs)

    def submit(
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
            A synchronous Job handle representing the submitted execution.
        """
        raise NotImplementedError("Remote task submission is not implemented yet.")

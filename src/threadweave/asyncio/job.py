from __future__ import annotations

from typing import Generic, TypeVar

from threadweave._internal.job import BaseJob

R = TypeVar("R")


class Job(BaseJob[R], Generic[R]):
    """
    Asynchronous ThreadWeave Job.

    `Job` is the concrete job handle exposed by the asyncio ThreadWeave API.

    It represents an execution submitted to the ThreadWeave Core and provides
    asynchronous operations for interacting with that execution.

    Notes
    -----
    Jobs are normally created by submitting an asynchronous Task::

        job = await task.submit(...)

    The synchronous API provides its own Job implementation through the
    top-level ``threadweave`` package.
    """

    async def result(
        self,
        timeout: float | None = None,
    ) -> R:
        """
        Wait asynchronously for the Job to complete and return its result.

        Parameters
        ----------
        timeout:
            Maximum number of seconds to wait for completion.

            If ``None``, wait indefinitely.

        Returns
        -------
        R
            The value returned by the executed Task.
        """
        raise NotImplementedError("Waiting for remote jobs is not implemented yet.")

    async def cancel(self) -> None:
        """
        Request cancellation of the Job.

        The request is sent asynchronously to the ThreadWeave Core.
        """
        raise NotImplementedError("Remote job cancellation is not implemented yet.")

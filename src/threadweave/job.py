from __future__ import annotations

from typing import Generic, TypeVar

from threadweave._internal.job import BaseJob

R = TypeVar("R")


class Job(BaseJob[R], Generic[R]):
    """
    Synchronous ThreadWeave Job.

    `Job` is the concrete job handle exposed by the synchronous ThreadWeave
    API.

    It represents an execution submitted to the ThreadWeave Core and provides
    blocking operations for interacting with that execution.

    Notes
    -----
    Jobs are normally created by submitting a synchronous Task::

        job = task.submit(...)

    The asyncio API provides its own Job implementation through
    ``threadweave.asyncio``.
    """

    def result(
        self,
        timeout: float | None = None,
    ) -> R:
        """
        Wait for the Job to complete and return its result.

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

    def cancel(self) -> None:
        """
        Request cancellation of the Job.

        The request is sent synchronously to the ThreadWeave Core.
        """
        raise NotImplementedError("Remote job cancellation is not implemented yet.")

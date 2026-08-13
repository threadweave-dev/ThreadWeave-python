from __future__ import annotations

import json
import time
from time import monotonic
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from threadweave._internal.job import BaseJob
from threadweave.protocol.common import ProtocolClientError

if TYPE_CHECKING:
    from threadweave.app import ThreadWeave

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
        application = cast("ThreadWeave", self.task.application)
        deadline = None if timeout is None else monotonic() + timeout

        while True:
            remaining = None if deadline is None else deadline - monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"Job {self.id!r} did not complete in time")

            response = application.client.get_job(self.id, timeout=remaining)
            if response.state == "SUCCEEDED":
                if response.payload is None:
                    raise ProtocolClientError(
                        f"Job {self.id!r} succeeded without a result payload"
                    )
                if response.serialization_format != "json":
                    raise ProtocolClientError(
                        "Unsupported job result serialization format "
                        f"{response.serialization_format!r}"
                    )
                return cast(R, json.loads(response.payload))
            if response.state in {"FAILED", "CANCELLED", "REJECTED", "TIMED_OUT"}:
                detail = f": {response.failure}" if response.failure else ""
                raise ProtocolClientError(
                    f"Job {self.id!r} ended in state {response.state}{detail}"
                )

            time.sleep(0.1 if remaining is None else min(0.1, remaining))

    def cancel(self) -> None:
        """
        Request cancellation of the Job.

        The request is sent synchronously to the ThreadWeave Core.
        """
        raise NotImplementedError("Remote job cancellation is not implemented yet.")

from __future__ import annotations

import asyncio
import json
from time import monotonic
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from threadweave._internal.job import BaseJob
from threadweave.protocol.common import ProtocolClientError

if TYPE_CHECKING:
    from threadweave.asyncio.app import ThreadWeave

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
        application = cast("ThreadWeave", self.task.application)
        deadline = None if timeout is None else monotonic() + timeout

        while True:
            remaining = None if deadline is None else deadline - monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"Job {self.id!r} did not complete in time")

            response = await application.client.get_job(
                self.id,
                timeout=remaining,
            )
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
            if response.state in {"FAILED", "CANCELLED", "REJECTED"}:
                detail = f": {response.failure}" if response.failure else ""
                raise ProtocolClientError(
                    f"Job {self.id!r} ended in state {response.state}{detail}"
                )

            await asyncio.sleep(
                0.1 if remaining is None else min(0.1, remaining)
            )

    async def cancel(self) -> None:
        """
        Request cancellation of the Job.

        The request is sent asynchronously to the ThreadWeave Core.
        """
        raise NotImplementedError("Remote job cancellation is not implemented yet.")

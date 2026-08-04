from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Coroutine, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

if TYPE_CHECKING:
    from threadweave.protocol.client import ProtocolClient


R = TypeVar("R")
T = TypeVar("T")


class JobState(StrEnum):
    """
    Client-side representation of the canonical ThreadWeave Job state.

    The Core remains the source of truth. The Python runtime must not infer
    transitions that have not been reported by the Core.
    """

    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"

    @property
    def is_terminal(self) -> bool:
        """Return whether no further execution transition is expected."""
        return self in {
            JobState.SUCCEEDED,
            JobState.FAILED,
            JobState.CANCELLED,
            JobState.TIMED_OUT,
        }

    @property
    def is_successful(self) -> bool:
        """Return whether the Job completed successfully."""
        return self is JobState.SUCCEEDED


@dataclass(frozen=True, slots=True)
class RemoteErrorData:
    """
    Serialized error information returned by the ThreadWeave Core.

    This representation must remain language-neutral. It must not rely on
    Python exception pickling or require the originating runtime to be Python.
    """

    type: str
    message: str
    details: Mapping[str, Any]
    traceback: str | None = None

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> RemoteErrorData:
        return cls(
            type=str(payload.get("type", "RemoteError")),
            message=str(
                payload.get(
                    "message",
                    "The remote Job failed without an error message.",
                )
            ),
            details=dict(payload.get("details") or {}),
            traceback=(
                str(payload["traceback"])
                if payload.get("traceback") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class JobSnapshot:
    """
    Immutable snapshot of a Job as reported by the ThreadWeave Core.
    """

    id: str
    task_id: str
    state: JobState
    attempt: int
    created_at: datetime | None = None
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    metadata: Mapping[str, Any] | None = None
    error: RemoteErrorData | None = None

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> JobSnapshot:
        try:
            state = JobState(str(payload["state"]))
        except KeyError as exc:
            raise ValueError("Job payload is missing the 'state' field.") from exc
        except ValueError as exc:
            raise ValueError(
                f"Unknown Job state: {payload.get('state')!r}."
            ) from exc

        error_payload = payload.get("error")

        return cls(
            id=str(payload["id"]),
            task_id=str(payload["task_id"]),
            state=state,
            attempt=int(payload.get("attempt", 0)),
            created_at=_parse_datetime(payload.get("created_at")),
            scheduled_at=_parse_datetime(payload.get("scheduled_at")),
            started_at=_parse_datetime(payload.get("started_at")),
            completed_at=_parse_datetime(payload.get("completed_at")),
            metadata=dict(payload.get("metadata") or {}),
            error=(
                RemoteErrorData.from_payload(error_payload)
                if isinstance(error_payload, Mapping)
                else None
            ),
        )


class JobError(RuntimeError):
    """Base exception for errors surfaced through a Job handle."""

    def __init__(
        self,
        message: str,
        *,
        job_id: str,
        state: JobState | None = None,
    ) -> None:
        super().__init__(message)
        self.job_id = job_id
        self.state = state


class JobFailedError(JobError):
    """Raised when a remote Job finishes in the failed state."""

    def __init__(
        self,
        *,
        job_id: str,
        remote_error: RemoteErrorData | None,
    ) -> None:
        if remote_error is None:
            message = f"Job {job_id!r} failed."
        else:
            message = (
                f"Job {job_id!r} failed with "
                f"{remote_error.type}: {remote_error.message}"
            )

        super().__init__(
            message,
            job_id=job_id,
            state=JobState.FAILED,
        )

        self.remote_error = remote_error


class JobCancelledError(JobError):
    """Raised when a Job is cancelled before producing a result."""

    def __init__(self, *, job_id: str) -> None:
        super().__init__(
            f"Job {job_id!r} was cancelled.",
            job_id=job_id,
            state=JobState.CANCELLED,
        )


class JobTimedOutError(JobError):
    """
    Raised when the remote execution itself reaches its configured timeout.

    This is distinct from ``asyncio.TimeoutError``, which means the local
    caller stopped waiting before the remote Job reached a terminal state.
    """

    def __init__(self, *, job_id: str) -> None:
        super().__init__(
            f"Job {job_id!r} exceeded its execution timeout.",
            job_id=job_id,
            state=JobState.TIMED_OUT,
        )


class JobProtocolError(JobError):
    """Raised when the Core returns an invalid or incomplete Job response."""


class Job(Generic[R]):
    """
    Handle to a Job managed by the ThreadWeave Core.

    A Job is created when a Task is submitted. This object exposes operations
    against that remote Job without owning its lifecycle or execution.

    Parameters
    ----------
    id:
        Canonical Job identifier returned by the Core.
    task_id:
        Canonical identifier of the submitted Task.
    client:
        Protocol client used to communicate with the Core.
    """

    def __init__(
        self,
        *,
        id: str,
        task_id: str,
        client: ProtocolClient,
    ) -> None:
        if not id:
            raise ValueError("Job id cannot be empty.")

        if not task_id:
            raise ValueError("Task id cannot be empty.")

        self._id = id
        self._task_id = task_id
        self._client = client

    @property
    def id(self) -> str:
        """Return the canonical Job identifier."""
        return self._id

    @property
    def task_id(self) -> str:
        """Return the canonical Task identifier."""
        return self._task_id

    def status(self) -> JobSnapshot:
        """Synchronously fetch the latest Job state."""
        return _run_sync(self.astatus())

    async def astatus(self) -> JobSnapshot:
        """
        Fetch the latest Job state from the ThreadWeave Core.

        The returned snapshot is immutable and represents only the state
        observed at the time of this request.
        """

        payload = await self._client.get_job(self._id)
        snapshot = JobSnapshot.from_payload(payload)

        if snapshot.id != self._id:
            raise JobProtocolError(
                (
                    f"The Core returned Job {snapshot.id!r} while "
                    f"{self._id!r} was requested."
                ),
                job_id=self._id,
            )

        if snapshot.task_id != self._task_id:
            raise JobProtocolError(
                (
                    f"Job {self._id!r} references Task "
                    f"{snapshot.task_id!r}, expected {self._task_id!r}."
                ),
                job_id=self._id,
                state=snapshot.state,
            )

        return snapshot

    def result(
        self,
        *,
        timeout: float | None = None,
    ) -> R:
        """Synchronously wait for and return the Job result."""
        return _run_sync(self.aresult(timeout=timeout))

    async def aresult(
        self,
        *,
        timeout: float | None = None,
    ) -> R:
        """
        Wait for the Job to complete and return its deserialized result.

        Parameters
        ----------
        timeout:
            Maximum local waiting time in seconds. This does not alter the
            remote Job execution timeout and does not cancel the Job when the
            local wait expires.

        Raises
        ------
        asyncio.TimeoutError
            If the caller-defined waiting timeout expires.
        JobFailedError
            If the remote Job fails.
        JobCancelledError
            If the remote Job is cancelled.
        JobTimedOutError
            If the remote execution reaches its configured timeout.
        JobProtocolError
            If the Core returns an inconsistent terminal response.
        """

        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(
                timeout,
                (int, float),
            ):
                raise TypeError("timeout must be a number of seconds.")

            if timeout <= 0:
                raise ValueError("timeout must be greater than zero.")

        waiter = self._wait_for_result()

        if timeout is None:
            return await waiter

        async with asyncio.timeout(timeout):
            return await waiter

    def wait(
        self,
        *,
        timeout: float | None = None,
    ) -> JobSnapshot:
        """Synchronously wait until the Job reaches a terminal state."""
        return _run_sync(self.await_terminal(timeout=timeout))

    async def await_terminal(
        self,
        *,
        timeout: float | None = None,
    ) -> JobSnapshot:
        """
        Wait until the Job reaches a terminal state.

        Unlike ``result()``, this method never raises merely because the remote
        Job failed, was cancelled, or timed out. It returns the terminal
        snapshot so that callers can inspect the state explicitly.
        """

        waiter = self._wait_for_terminal_state()

        if timeout is None:
            return await waiter

        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise TypeError("timeout must be a number of seconds.")

        if timeout <= 0:
            raise ValueError("timeout must be greater than zero.")

        async with asyncio.timeout(timeout):
            return await waiter

    def cancel(self) -> bool:
        """Synchronously request cancellation of the Job."""
        return _run_sync(self.acancel())

    async def acancel(self) -> bool:
        """
        Request cancellation of the Job.

        Returns
        -------
        bool
            ``True`` when the cancellation request was accepted. Acceptance
            does not imply that execution has already stopped.

            ``False`` typically means the Job was already terminal or could
            not be cancelled by the Core.
        """

        response = await self._client.cancel_job(self._id)
        return bool(response.accepted)

    def events(self) -> Iterator[Mapping[str, Any]]:
        """
        Synchronously stream Job events.

        Use ``aevents()`` when an asyncio event loop is running.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError(
                "Job.events() cannot be called from a running event loop; "
                "use 'async for event in job.aevents()' instead."
            )

        loop = asyncio.new_event_loop()
        stream = self.aevents()
        try:
            while True:
                try:
                    yield loop.run_until_complete(anext(stream))
                except StopAsyncIteration:
                    return
        finally:
            loop.run_until_complete(stream.aclose())
            loop.close()

    async def aevents(self) -> AsyncIterator[Mapping[str, Any]]:
        """
        Stream Job events emitted by the ThreadWeave Core.

        Events are yielded as protocol-level mappings until a terminal event
        is observed or the transport closes the stream.

        Higher-level typed event classes can be added once the canonical event
        protocol is finalized.
        """

        async for event in self._client.watch_job(self._id):
            yield event

    def __await__(self) -> Iterator[Any]:
        """Awaiting a Job is equivalent to awaiting ``aresult()``."""
        return self.aresult().__await__()

    async def _wait_for_result(self) -> R:
        response = await self._client.wait_for_job_result(self._id)

        try:
            state = JobState(str(response.state))
        except ValueError as exc:
            raise JobProtocolError(
                f"The Core returned an unknown state: {response.state!r}.",
                job_id=self._id,
            ) from exc

        if state is JobState.SUCCEEDED:
            if not getattr(response, "has_result", True):
                raise JobProtocolError(
                    "The Job succeeded but no result was returned.",
                    job_id=self._id,
                    state=state,
                )

            return cast(R, response.result)

        if state is JobState.FAILED:
            error_payload = getattr(response, "error", None)
            remote_error = (
                RemoteErrorData.from_payload(error_payload)
                if isinstance(error_payload, Mapping)
                else None
            )

            raise JobFailedError(
                job_id=self._id,
                remote_error=remote_error,
            )

        if state is JobState.CANCELLED:
            raise JobCancelledError(job_id=self._id)

        if state is JobState.TIMED_OUT:
            raise JobTimedOutError(job_id=self._id)

        raise JobProtocolError(
            (
                f"The result endpoint returned non-terminal state "
                f"{state.value!r} for Job {self._id!r}."
            ),
            job_id=self._id,
            state=state,
        )

    async def _wait_for_terminal_state(self) -> JobSnapshot:
        payload = await self._client.wait_for_job(self._id)
        snapshot = JobSnapshot.from_payload(payload)

        if not snapshot.state.is_terminal:
            raise JobProtocolError(
                (
                    f"The wait endpoint returned non-terminal state "
                    f"{snapshot.state.value!r}."
                ),
                job_id=self._id,
                state=snapshot.state,
            )

        return snapshot

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"id={self._id!r}, "
            f"task_id={self._task_id!r}"
            f")"
        )


def _parse_datetime(value: Any) -> datetime | None:
    """
    Parse an ISO 8601 datetime returned by the protocol.

    Protocol models should eventually own this conversion. Keeping it here
    makes this initial Job implementation usable without coupling it to a
    specific serialization library.
    """

    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if not isinstance(value, str):
        raise TypeError(
            "Job datetime fields must be datetime objects, ISO 8601 strings, "
            "or null."
        )

    normalized = value.strip()

    if not normalized:
        raise ValueError("Job datetime fields cannot be empty strings.")

    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"Invalid ISO 8601 datetime value: {value!r}."
        ) from exc


def _run_sync(coroutine: Coroutine[Any, Any, T]) -> T:
    """Run an async Job operation from synchronous Python."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    coroutine.close()
    raise RuntimeError(
        "Synchronous Job methods cannot be called from a running event loop; "
        "use the corresponding asynchronous method instead."
    )

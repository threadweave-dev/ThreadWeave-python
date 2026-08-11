from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from threadweave._internal.task import BaseTask


R = TypeVar("R")


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
class JobResultRef(Generic[R]):
    job_id: str


class BaseJob(Generic[R]):
    """
    Common base class for ThreadWeave Job implementations.

    `BaseJob` represents a submitted ThreadWeave execution independently of
    whether it is manipulated through the synchronous or asynchronous Python
    API.

    The class contains only state and behavior shared by both execution models.
    It is intentionally unaware of transport details and does not perform any
    communication with the ThreadWeave Core.

    Concrete Job implementations are responsible for operations involving I/O,
    such as retrieving execution state, waiting for completion, obtaining the
    result, or cancelling the job.

    Parameters
    ----------
    id:
        Canonical identifier assigned to the Job by the ThreadWeave Core.

    task:
        Task from which this Job was created.

    Notes
    -----
    The generic ``R`` parameter represents the result type produced by the
    associated Task.

    A synchronous implementation may expose methods such as::

        job.result()
        job.cancel()

    while an asyncio implementation may expose the same API surface using
    awaitable methods::

        await job.result()
        await job.cancel()

    These operations deliberately do not belong to `BaseJob`, keeping the
    common representation independent of the execution model.
    """

    def __init__(
        self,
        *,
        id: str,
        task: BaseTask[..., R],
    ) -> None:
        self._id = id
        self._task = task

    @property
    def id(self) -> str:
        """
        Return the canonical Job identifier.
        """
        return self._id

    @property
    def result_ref(self) -> JobResultRef[R]:
        return JobResultRef(job_id=self.id)

    @property
    def task(self) -> BaseTask[..., R]:
        """
        Return the Task from which this Job was submitted.
        """
        return self._task

    @property
    def task_id(self) -> str:
        """
        Return the canonical identifier of the associated Task.
        """
        return self._task.id

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"id={self._id!r}, "
            f"task={self._task.id!r}"
            f")"
        )

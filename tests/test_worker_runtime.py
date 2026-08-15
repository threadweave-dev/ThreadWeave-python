from __future__ import annotations

from typing import Any

from threadweave_protocols.execution.v1 import execution_pb2, jobs_pb2
from threadweave_protocols.runtime.v1 import worker_pb2

from threadweave import ThreadWeave
from threadweave.runtime.worker import Worker


class RecordingClient:
    def __init__(self) -> None:
        self.reports: list[tuple[int, Any]] = []

    def connect(self, timeout: float = 10.0) -> None:
        pass

    def acquire_execution(self) -> worker_pb2.AssignExecutionRequest | None:
        return None

    def report_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        state: int,
        outcome: Any = None,
    ) -> None:
        self.reports.append((state, outcome))

    def close(self) -> None:
        pass


def assignment(task: str, payload: bytes) -> worker_pb2.AssignExecutionRequest:
    return worker_pb2.AssignExecutionRequest(
        assignment_id="assignment-1",
        execution_id="execution-1",
        job_id="job-1",
        task=jobs_pb2.TaskIdentity(
            namespace="default", application="example", name=task
        ),
        arguments=payload,
        serialization_format="json",
    )


def test_assignment_resolves_task_deserializes_arguments_and_returns_json() -> None:
    application = ThreadWeave("example")

    @application.task
    def add(a: int, b: int) -> int:
        return a + b

    client = RecordingClient()
    Worker(application, client).execute(
        assignment("add", b'{"args":[40,2],"kwargs":{}}')
    )

    assert application.discover_tasks() == (add,)
    assert [state for state, _ in client.reports] == [
        execution_pb2.EXECUTION_STATE_RUNNING,
        execution_pb2.EXECUTION_STATE_SUCCEEDED,
    ]
    assert client.reports[-1][1].payload == b"42"
    assert client.reports[-1][1].serialization_format == "json"


def test_task_exception_becomes_failed_report() -> None:
    application = ThreadWeave("example")

    @application.task
    def explode() -> None:
        raise ValueError("bad input")

    client = RecordingClient()
    Worker(application, client).execute(assignment("explode", b'{"args":[]}'))

    state, outcome = client.reports[-1]
    assert state == execution_pb2.EXECUTION_STATE_FAILED
    assert outcome.failure.code == "ValueError"
    assert outcome.failure.message == "bad input"


def test_unknown_task_becomes_failed_report() -> None:
    application = ThreadWeave("example")
    client = RecordingClient()

    Worker(application, client).execute(assignment("missing", b'{"args":[]}'))

    state, outcome = client.reports[-1]
    assert state == execution_pb2.EXECUTION_STATE_FAILED
    assert outcome.failure.code == "TaskNotRegisteredError"
    assert "default.example.missing" in outcome.failure.message

from __future__ import annotations

from typing import Any

import pytest
from threadweave_protocols.execution.v1 import execution_pb2, jobs_pb2
from threadweave_protocols.runtime.v1 import worker_pb2

from threadweave import ThreadWeave
from threadweave.runtime.worker import PythonRuntime


class RecordingClient:
    def __init__(self) -> None:
        self.reports: list[tuple[int, Any]] = []
        self.closed = False

    def connect(self, timeout: float = 10.0) -> None:
        pass

    def acquire_execution(
        self, *, timeout: float | None = None
    ) -> worker_pb2.AssignExecutionRequest | None:
        return None

    def start_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        *,
        timeout: float | None = None,
    ) -> None:
        self.reports.append((execution_pb2.EXECUTION_STATE_RUNNING, None))

    def complete_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        result: Any,
        *,
        timeout: float | None = None,
    ) -> None:
        self.reports.append((execution_pb2.EXECUTION_STATE_SUCCEEDED, result))

    def fail_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        failure: Any,
        *,
        timeout: float | None = None,
    ) -> None:
        outcome = type("Outcome", (), {"failure": failure})()
        self.reports.append((execution_pb2.EXECUTION_STATE_FAILED, outcome))

    def close(self) -> None:
        self.closed = True


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


def test_assignment_resolves_task_deserializes_arguments_and_returns_json(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO")
    application = ThreadWeave("example")

    @application.task
    def add(a: int, b: int) -> int:
        return a + b

    client = RecordingClient()
    PythonRuntime(application, client).execute(
        assignment("add", b'{"args":[40,2],"kwargs":{}}')
    )

    assert application.discover_tasks() == (add,)
    assert [state for state, _ in client.reports] == [
        execution_pb2.EXECUTION_STATE_RUNNING,
        execution_pb2.EXECUTION_STATE_SUCCEEDED,
    ]
    assert client.reports[-1][1].payload == b"42"
    assert client.reports[-1][1].serialization_format == "json"
    assert "Starting task default.example.add" in caplog.text
    assert "Task default.example.add succeeded" in caplog.text
    assert "Execution execution-1 result reported to Worker" in caplog.text


def test_task_exception_becomes_failed_report(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO")
    application = ThreadWeave("example")

    @application.task
    def explode() -> None:
        raise ValueError("bad input")

    client = RecordingClient()
    PythonRuntime(application, client).execute(assignment("explode", b'{"args":[]}'))

    state, outcome = client.reports[-1]
    assert state == execution_pb2.EXECUTION_STATE_FAILED
    assert outcome.failure.code == "ValueError"
    assert outcome.failure.message == "bad input"
    assert "Task default.example.explode failed: ValueError: bad input" in caplog.text
    assert "Execution execution-1 failure reported to Worker" in caplog.text


def test_task_arguments_and_result_payload_are_not_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO")
    application = ThreadWeave("example")

    @application.task
    def echo(value: str) -> str:
        return f"result-{value}"

    PythonRuntime(application, RecordingClient()).execute(
        assignment("echo", b'{"args":["secret-argument"]}')
    )

    assert "secret-argument" not in caplog.text
    assert "result-secret-argument" not in caplog.text


def test_unknown_task_becomes_failed_report() -> None:
    application = ThreadWeave("example")
    client = RecordingClient()

    PythonRuntime(application, client).execute(assignment("missing", b'{"args":[]}'))

    state, outcome = client.reports[-1]
    assert state == execution_pb2.EXECUTION_STATE_FAILED
    assert outcome.failure.code == "TaskNotRegisteredError"
    assert "default.example.missing" in outcome.failure.message


def test_keyword_arguments_are_deserialized() -> None:
    application = ThreadWeave("example")

    @application.task
    def add(a: int, *, b: int) -> int:
        return a + b

    client = RecordingClient()
    PythonRuntime(application, client).execute(
        assignment("add", b'{"args":[40],"kwargs":{"b":2}}')
    )

    assert client.reports[-1][1].payload == b"42"


def test_unsupported_serialization_format_becomes_failed_report() -> None:
    application = ThreadWeave("example")

    @application.task
    def add(a: int, b: int) -> int:
        return a + b

    client = RecordingClient()
    work = assignment("add", b"data")
    work.serialization_format = "msgpack"

    PythonRuntime(application, client).execute(work)

    state, outcome = client.reports[-1]
    assert state == execution_pb2.EXECUTION_STATE_FAILED
    assert outcome.failure.code == "ValueError"
    assert "unsupported argument serialization format" in outcome.failure.message


def test_run_forever_closes_client_on_keyboard_interrupt() -> None:
    application = ThreadWeave("example")
    client = RecordingClient()

    def interrupt(*, timeout: float | None = None) -> None:
        raise KeyboardInterrupt

    client.acquire_execution = interrupt  # type: ignore[method-assign]

    try:
        PythonRuntime(application, client).run_forever()
    except KeyboardInterrupt:
        pass

    assert client.closed


def test_run_forever_logs_acquired_execution(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO")
    application = ThreadWeave("example")

    @application.task
    def add(a: int, b: int) -> int:
        return a + b

    client = RecordingClient()
    assignments = iter([assignment("add", b'{"args":[1,2]}')])

    def acquire(*, timeout: float | None = None) -> worker_pb2.AssignExecutionRequest:
        try:
            return next(assignments)
        except StopIteration:
            raise KeyboardInterrupt from None

    client.acquire_execution = acquire  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt):
        PythonRuntime(application, client).run_forever()

    assert "Acquired execution execution-1 for default.example.add" in caplog.text

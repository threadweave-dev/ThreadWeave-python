from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
from threadweave_protocols.execution.v1 import jobs_pb2
from threadweave_protocols.runtime.v1 import worker_pb2

from threadweave import ThreadWeave
from threadweave.runtime.worker import PythonRuntime


class RecordingClient:
    def __init__(self, commands: list[worker_pb2.WorkerCommand] | None = None) -> None:
        self.commands = commands or []
        self.events_sent: list[tuple[str, Any]] = []
        self.closed = False

    async def connect(self, timeout: float = 10.0) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    async def execution_started(self, work: Any) -> None:
        self.events_sent.append(("started", work))

    async def execution_metrics(self, work: Any, **metrics: Any) -> None:
        self.events_sent.append(("metrics", metrics))

    async def execution_completed(self, work: Any, result: Any) -> None:
        self.events_sent.append(("completed", result))

    async def execution_failed(self, work: Any, failure: Any) -> None:
        self.events_sent.append(("failed", failure))

    async def events(self) -> AsyncIterator[worker_pb2.WorkerCommand]:
        for command in self.commands:
            yield command


def assignment(name: str = "add") -> worker_pb2.AssignExecutionRequest:
    return worker_pb2.AssignExecutionRequest(
        assignment_id="a",
        execution_id="e",
        serialization_format="json",
        arguments=b'{"args":[40,2],"kwargs":{}}',
        task=jobs_pb2.TaskIdentity(
            namespace="default", application="example", name=name
        ),
    )


@pytest.mark.asyncio
async def test_sync_task_reports_metrics_and_completion() -> None:
    app = ThreadWeave("example")

    @app.task
    def add(a: int, b: int) -> int:
        return a + b

    client = RecordingClient()
    await PythonRuntime(app, client).execute(assignment())
    assert [name for name, _ in client.events_sent] == [
        "started",
        "metrics",
        "completed",
    ]
    assert client.events_sent[-1][1].payload == b"42"
    assert set(client.events_sent[1][1]) >= {
        "elapsed_ms",
        "deserialization_ms",
        "execution_ms",
        "serialization_ms",
    }


@pytest.mark.asyncio
async def test_task_failure_is_reported() -> None:
    app = ThreadWeave("example")

    @app.task
    def explode(a: int, b: int) -> None:
        raise ValueError("bad input")

    client = RecordingClient()
    await PythonRuntime(app, client).execute(assignment("explode"))
    assert client.events_sent[-1][0] == "failed"
    assert client.events_sent[-1][1].code == "ValueError"


@pytest.mark.asyncio
async def test_sync_task_does_not_block_event_loop() -> None:
    app = ThreadWeave("example")

    @app.task
    def slow(a: int, b: int) -> int:
        time.sleep(0.05)
        return a + b

    client = RecordingClient()
    execution = asyncio.create_task(
        PythonRuntime(app, client).execute(assignment("slow"))
    )
    await asyncio.sleep(0.005)
    assert not execution.done()
    await execution


@pytest.mark.asyncio
async def test_runtime_handles_multiple_assignments_and_closes() -> None:
    app = ThreadWeave("example")

    @app.task
    def add(a: int, b: int) -> int:
        return a + b

    commands = [
        worker_pb2.WorkerCommand(assign_execution=assignment()) for _ in range(2)
    ]
    client = RecordingClient(commands)
    await PythonRuntime(app, client).run_forever()
    assert client.closed
    assert [name for name, _ in client.events_sent].count("completed") == 2


@pytest.mark.asyncio
async def test_cancel_command_cancels_awaiting_execution() -> None:
    app = ThreadWeave("example")

    @app.task
    def slow(a: int, b: int) -> int:
        time.sleep(0.05)
        return a + b

    commands = [
        worker_pb2.WorkerCommand(assign_execution=assignment("slow")),
        worker_pb2.WorkerCommand(
            cancel_execution=worker_pb2.CancelExecution(
                assignment_id="a", execution_id="e"
            )
        ),
    ]
    client = RecordingClient(commands)
    await PythonRuntime(app, client).run_forever()
    assert not any(name == "completed" for name, _ in client.events_sent)

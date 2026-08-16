from __future__ import annotations

from typing import Any

import pytest
from threadweave_protocols.common.v1 import errors_pb2
from threadweave_protocols.execution.v1 import results_pb2
from threadweave_protocols.runtime.v1 import worker_pb2

from threadweave.protocol.common import ProtocolClientError
from threadweave.protocol.runtime_client import RuntimeProtocolClient


class FakeCall:
    def __init__(self, commands: list[worker_pb2.WorkerCommand] | None = None) -> None:
        self.commands = commands or []
        self.writes: list[worker_pb2.RuntimeEvent] = []
        self.done = False

    def __aiter__(self) -> Any:
        async def values() -> Any:
            for command in self.commands:
                yield command

        return values()

    async def write(self, event: worker_pb2.RuntimeEvent) -> None:
        self.writes.append(event)

    async def done_writing(self) -> None:
        self.done = True


def assignment() -> worker_pb2.AssignExecutionRequest:
    return worker_pb2.AssignExecutionRequest(
        assignment_id="assignment-1", execution_id="execution-1"
    )


@pytest.mark.asyncio
async def test_events_yield_worker_commands() -> None:
    expected = worker_pb2.WorkerCommand(assign_execution=assignment())
    client = RuntimeProtocolClient()
    client._call = FakeCall([expected])
    assert [command async for command in client.events()] == [expected]


@pytest.mark.asyncio
async def test_lifecycle_events_are_sequenced_on_one_writer() -> None:
    client = RuntimeProtocolClient()
    call = FakeCall()
    client._call = call
    client._writer_task = __import__("asyncio").create_task(client._writer())
    work = assignment()
    await client.execution_started(work)
    await client.execution_metrics(work, execution_ms=7)
    await client.execution_completed(
        work, results_pb2.JobResult(payload=b"42", serialization_format="json")
    )
    await client._outgoing.put(None)
    await client._writer_task

    assert [event.WhichOneof("payload") for event in call.writes] == [
        "execution_started",
        "execution_metrics",
        "execution_completed",
    ]
    assert [
        getattr(event, event.WhichOneof("payload")).sequence_number
        for event in call.writes
    ] == [1, 2, 3]
    assert call.writes[1].execution_metrics.execution_ms == 7


@pytest.mark.asyncio
async def test_failure_reuses_protocol_error() -> None:
    client = RuntimeProtocolClient()
    call = FakeCall()
    client._call = call
    client._writer_task = __import__("asyncio").create_task(client._writer())
    await client.execution_started(assignment())
    await client.execution_failed(
        assignment(), errors_pb2.Error(code="ValueError", message="bad")
    )
    await client._outgoing.put(None)
    await client._writer_task
    assert call.writes[-1].execution_failed.failure.code == "ValueError"


@pytest.mark.asyncio
async def test_operations_require_connected_client() -> None:
    with pytest.raises(ProtocolClientError, match="not connected"):
        await RuntimeProtocolClient().execution_started(assignment())

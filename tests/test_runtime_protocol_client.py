from __future__ import annotations

from typing import Any

import pytest
from threadweave_protocols.common.v1 import errors_pb2
from threadweave_protocols.execution.v1 import execution_pb2, results_pb2
from threadweave_protocols.runtime.v1 import runtime_pb2, worker_pb2

from threadweave.protocol.common import ProtocolClientError
from threadweave.protocol.runtime_client import RuntimeProtocolClient


class FakeStub:
    def __init__(self, assignment: worker_pb2.AssignExecutionRequest | None = None):
        self.assignment = assignment
        self.acquire_calls: list[tuple[Any, float | None]] = []
        self.reports: list[tuple[worker_pb2.ReportExecutionRequest, float | None]] = []
        self.accept_reports = True

    def AcquireExecution(self, request: Any, timeout: float | None) -> Any:
        self.acquire_calls.append((request, timeout))
        response = runtime_pb2.AcquireExecutionResponse()
        if self.assignment is not None:
            response.assignment.CopyFrom(self.assignment)
        return response

    def ReportExecution(self, request: Any, timeout: float | None) -> Any:
        self.reports.append((request, timeout))
        return runtime_pb2.ReportExecutionResponse(accepted=self.accept_reports)


def make_client(stub: FakeStub) -> RuntimeProtocolClient:
    client = RuntimeProtocolClient("127.0.0.1:50052")
    client._stub = stub
    return client


def test_acquire_uses_generated_request_and_returns_assignment() -> None:
    assignment = worker_pb2.AssignExecutionRequest(
        assignment_id="assignment-1", execution_id="execution-1"
    )
    stub = FakeStub(assignment)
    result = make_client(stub).acquire_execution(timeout=12.0)
    assert result == assignment
    request, timeout = stub.acquire_calls[0]
    assert isinstance(request, runtime_pb2.AcquireExecutionRequest)
    assert not request.HasField("worker")
    assert timeout == 12.0


def test_acquire_returns_none_when_no_work_is_available() -> None:
    assert make_client(FakeStub()).acquire_execution() is None


def test_lifecycle_methods_build_sequenced_generated_reports() -> None:
    stub = FakeStub()
    client = make_client(stub)
    assignment = worker_pb2.AssignExecutionRequest(
        assignment_id="assignment-1", execution_id="execution-1"
    )
    result = results_pb2.JobResult(payload=b"42", serialization_format="json")
    failure = errors_pb2.Error(code="ValueError", message="bad input")
    client.start_execution(assignment)
    client.complete_execution(assignment, result)
    client.fail_execution(assignment, failure)
    started, completed, failed = [request for request, _ in stub.reports]
    assert started.state == execution_pb2.EXECUTION_STATE_RUNNING
    assert started.sequence_number == 1
    assert not started.HasField("outcome")
    assert completed.state == execution_pb2.EXECUTION_STATE_SUCCEEDED
    assert completed.sequence_number == 2
    assert completed.outcome == result
    assert failed.state == execution_pb2.EXECUTION_STATE_FAILED
    assert failed.sequence_number == 2
    assert failed.outcome.failure == failure


def test_rejected_report_uses_protocol_client_error() -> None:
    stub = FakeStub()
    stub.accept_reports = False
    assignment = worker_pb2.AssignExecutionRequest(
        assignment_id="assignment-1", execution_id="execution-1"
    )
    with pytest.raises(ProtocolClientError, match="rejected"):
        make_client(stub).start_execution(assignment)


def test_operations_require_a_connected_client() -> None:
    with pytest.raises(ProtocolClientError, match="not connected"):
        RuntimeProtocolClient("127.0.0.1:50052").acquire_execution()

from __future__ import annotations

import uuid
from typing import Any

import grpc  # type: ignore[import-untyped]
from google.protobuf.timestamp_pb2 import Timestamp
from threadweave_protocols.common.v1 import errors_pb2
from threadweave_protocols.execution.v1 import execution_pb2, results_pb2
from threadweave_protocols.runtime.v1 import runtime_pb2, runtime_pb2_grpc, worker_pb2

from threadweave.protocol.common import (
    BaseProtocolClient,
    ProtocolClientError,
    ProtocolUnavailableError,
    grpc_target,
    raise_rpc_error,
)


class RuntimeProtocolClient(BaseProtocolClient):
    """Blocking transport for a Rust Worker's runtime-facing API."""

    def __init__(self, endpoint: str | None = None) -> None:
        super().__init__(endpoint)
        self._channel: grpc.Channel | None = None
        self._stub: Any = None

    def connect(self, timeout: float = 10.0) -> None:
        if self._channel is not None:
            return
        channel = grpc.insecure_channel(grpc_target(self._endpoint))
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
        except grpc.FutureTimeoutError as error:
            channel.close()
            raise ProtocolUnavailableError(
                f"gRPC channel unavailable at {self._endpoint}"
            ) from error
        self._channel = channel
        self._stub = runtime_pb2_grpc.RuntimeServiceStub(  # type: ignore[no-untyped-call]
            channel
        )

    def acquire_execution(
        self, *, timeout: float | None = None
    ) -> worker_pb2.AssignExecutionRequest | None:
        stub = self._require_stub()
        try:
            response = stub.AcquireExecution(
                # The worker owns cluster identity. The current POC protocol still
                # has a legacy WorkerRegistration field, but the worker runtime
                # endpoint deliberately ignores it.
                runtime_pb2.AcquireExecutionRequest(),
                timeout=timeout,
            )
        except grpc.RpcError as error:
            raise_rpc_error(error, "AcquireExecution")
        return response.assignment if response.HasField("assignment") else None

    def start_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        *,
        timeout: float | None = None,
    ) -> None:
        self._report_execution(
            assignment,
            execution_pb2.EXECUTION_STATE_RUNNING,
            sequence_number=1,
            timeout=timeout,
        )

    def complete_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        result: results_pb2.JobResult,
        *,
        timeout: float | None = None,
    ) -> None:
        self._report_execution(
            assignment,
            execution_pb2.EXECUTION_STATE_SUCCEEDED,
            sequence_number=2,
            outcome=result,
            timeout=timeout,
        )

    def fail_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        failure: errors_pb2.Error,
        *,
        timeout: float | None = None,
    ) -> None:
        self._report_execution(
            assignment,
            execution_pb2.EXECUTION_STATE_FAILED,
            sequence_number=2,
            outcome=results_pb2.JobResult(failure=failure),
            timeout=timeout,
        )

    def _report_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        state: execution_pb2.ExecutionState,
        *,
        sequence_number: int,
        timeout: float | None,
        outcome: results_pb2.JobResult | None = None,
    ) -> None:
        stub = self._require_stub()
        observed_at = Timestamp()
        observed_at.GetCurrentTime()
        request = worker_pb2.ReportExecutionRequest(
            report_id=str(uuid.uuid4()),
            assignment_id=assignment.assignment_id,
            execution_id=assignment.execution_id,
            sequence_number=sequence_number,
            state=state,
            observed_at=observed_at,
        )
        if outcome is not None:
            request.outcome.CopyFrom(outcome)
        try:
            response = stub.ReportExecution(request, timeout=timeout)
        except grpc.RpcError as error:
            raise_rpc_error(error, "ReportExecution")
        if not response.accepted:
            raise ProtocolClientError("Core rejected the execution report")

    def _require_stub(self) -> Any:
        if self._stub is None:
            raise ProtocolClientError("runtime protocol client is not connected")
        return self._stub

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()
        self._channel = None
        self._stub = None

    def __enter__(self) -> RuntimeProtocolClient:
        self.connect()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


GrpcRuntimeClient = RuntimeProtocolClient

from __future__ import annotations

import json
import platform
import uuid
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Protocol

import grpc  # type: ignore[import-untyped]
from google.protobuf.timestamp_pb2 import Timestamp
from threadweave_protocols.common.v1 import errors_pb2
from threadweave_protocols.execution.v1 import execution_pb2, results_pb2
from threadweave_protocols.runtime.v1 import (
    runtime_pb2,
    runtime_pb2_grpc,
    worker_pb2,
)

from threadweave._internal.app import BaseThreadWeave
from threadweave.protocol.common import grpc_target, raise_rpc_error


class RuntimeClient(Protocol):
    def connect(self, timeout: float = 10.0) -> None: ...

    def acquire_execution(self) -> worker_pb2.AssignExecutionRequest | None: ...

    def report_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        state: execution_pb2.ExecutionState,
        outcome: results_pb2.JobResult | None = None,
    ) -> None: ...

    def close(self) -> None: ...


class GrpcRuntimeClient:
    """Blocking client for the Core's worker-facing runtime protocol."""

    def __init__(self, endpoint: str, worker_id: str | None = None) -> None:
        self._endpoint = endpoint
        self._channel: grpc.Channel | None = None
        self._stub: Any = None
        try:
            implementation_version = version("threadweave-python")
        except PackageNotFoundError:
            implementation_version = "unknown"
        self._worker = worker_pb2.WorkerRegistration(
            worker_id=worker_id or str(uuid.uuid4()),
            generation=str(uuid.uuid4()),
            implementation_version=implementation_version,
            protocol_versions=["v1"],
            executors=[
                worker_pb2.ExecutorDescriptor(
                    executor_id="python-sync",
                    runtime=f"python-{platform.python_version()}",
                    implementation_version=implementation_version,
                    serialization_formats=["json"],
                    concurrency_limit=1,
                )
            ],
        )

    def connect(self, timeout: float = 10.0) -> None:
        if self._channel is not None:
            return
        channel = grpc.insecure_channel(grpc_target(self._endpoint))
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
        except grpc.FutureTimeoutError as error:
            channel.close()
            raise RuntimeError(
                f"gRPC channel unavailable at {self._endpoint}"
            ) from error
        self._channel = channel
        self._stub = runtime_pb2_grpc.RuntimeServiceStub(  # type: ignore[no-untyped-call]
            channel
        )

    def acquire_execution(self) -> worker_pb2.AssignExecutionRequest | None:
        if self._stub is None:
            raise RuntimeError("runtime client is not connected")
        try:
            response = self._stub.AcquireExecution(
                runtime_pb2.AcquireExecutionRequest(worker=self._worker),
                timeout=None,
            )
        except grpc.RpcError as error:
            raise_rpc_error(error, "AcquireExecution")
        return response.assignment if response.HasField("assignment") else None

    def report_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        state: execution_pb2.ExecutionState,
        outcome: results_pb2.JobResult | None = None,
    ) -> None:
        if self._stub is None:
            raise RuntimeError("runtime client is not connected")
        observed_at = Timestamp()
        observed_at.GetCurrentTime()
        request = worker_pb2.ReportExecutionRequest(
            report_id=str(uuid.uuid4()),
            assignment_id=assignment.assignment_id,
            execution_id=assignment.execution_id,
            sequence_number=1,
            state=state,
            observed_at=observed_at,
        )
        if outcome is not None:
            request.outcome.CopyFrom(outcome)
        try:
            response = self._stub.ReportExecution(request)
        except grpc.RpcError as error:
            raise_rpc_error(error, "ReportExecution")
        if not response.accepted:
            raise RuntimeError("Core rejected the execution report")

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()
        self._channel = None
        self._stub = None


class Worker:
    """Execute one assigned synchronous task at a time in this process."""

    def __init__(
        self, application: BaseThreadWeave[Any], client: RuntimeClient
    ) -> None:
        self.application = application
        self.client = client

    def run_forever(self) -> None:
        self.client.connect()
        try:
            while True:
                assignment = self.client.acquire_execution()
                if assignment is not None:
                    self.execute(assignment)
        finally:
            self.client.close()

    def execute(self, assignment: worker_pb2.AssignExecutionRequest) -> None:
        self.client.report_execution(assignment, execution_pb2.EXECUTION_STATE_RUNNING)
        try:
            task_id = self._task_id(assignment)
            task = self.application.get_task(task_id)
            if assignment.serialization_format != "json":
                raise ValueError(
                    "unsupported argument serialization format "
                    f"{assignment.serialization_format!r}"
                )
            arguments = json.loads(assignment.arguments)
            if not isinstance(arguments, dict):
                raise ValueError("JSON arguments must be an object")
            args = arguments.get("args", [])
            kwargs = arguments.get("kwargs", {})
            if not isinstance(args, list) or not isinstance(kwargs, dict):
                raise ValueError(
                    "JSON arguments require list 'args' and object 'kwargs'"
                )
            result = task(*args, **kwargs)
            payload = json.dumps(result, separators=(",", ":")).encode()
        except Exception as error:
            failure = errors_pb2.Error(
                code=type(error).__name__,
                message=str(error),
            )
            self.client.report_execution(
                assignment,
                execution_pb2.EXECUTION_STATE_FAILED,
                results_pb2.JobResult(failure=failure),
            )
            return

        self.client.report_execution(
            assignment,
            execution_pb2.EXECUTION_STATE_SUCCEEDED,
            results_pb2.JobResult(
                payload=payload,
                serialization_format="json",
            ),
        )

    def _task_id(self, assignment: worker_pb2.AssignExecutionRequest) -> str:
        if not assignment.HasField("task"):
            raise LookupError("assignment does not contain a task identity")
        identity = assignment.task
        if identity.namespace and identity.application:
            return f"{identity.namespace}.{identity.application}.{identity.name}"
        return identity.name

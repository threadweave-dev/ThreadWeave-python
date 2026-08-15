from __future__ import annotations

import json
from typing import Any, Protocol

from threadweave_protocols.common.v1 import errors_pb2
from threadweave_protocols.execution.v1 import results_pb2
from threadweave_protocols.runtime.v1 import worker_pb2

from threadweave._internal.app import BaseThreadWeave
from threadweave.protocol.runtime_client import RuntimeProtocolClient


class RuntimeClient(Protocol):
    def connect(self, timeout: float = 10.0) -> None: ...
    def acquire_execution(
        self, *, timeout: float | None = None
    ) -> worker_pb2.AssignExecutionRequest | None: ...
    def start_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        *,
        timeout: float | None = None,
    ) -> None: ...
    def complete_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        result: results_pb2.JobResult,
        *,
        timeout: float | None = None,
    ) -> None: ...
    def fail_execution(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        failure: errors_pb2.Error,
        *,
        timeout: float | None = None,
    ) -> None: ...
    def close(self) -> None: ...


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
        self.client.start_execution(assignment)
        try:
            task = self.application.get_task(self._task_id(assignment))
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
            payload = json.dumps(task(*args, **kwargs), separators=(",", ":")).encode()
        except Exception as error:
            self.client.fail_execution(
                assignment,
                errors_pb2.Error(code=type(error).__name__, message=str(error)),
            )
            return
        self.client.complete_execution(
            assignment,
            results_pb2.JobResult(payload=payload, serialization_format="json"),
        )

    def _task_id(self, assignment: worker_pb2.AssignExecutionRequest) -> str:
        if not assignment.HasField("task"):
            raise LookupError("assignment does not contain a task identity")
        identity = assignment.task
        if identity.namespace and identity.application:
            return f"{identity.namespace}.{identity.application}.{identity.name}"
        return identity.name


GrpcRuntimeClient = RuntimeProtocolClient

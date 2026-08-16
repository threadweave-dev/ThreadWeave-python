from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any, Protocol

from threadweave_protocols.common.v1 import errors_pb2
from threadweave_protocols.execution.v1 import results_pb2
from threadweave_protocols.runtime.v1 import worker_pb2

from threadweave._internal.app import BaseThreadWeave
from threadweave.protocol.runtime_client import RuntimeProtocolClient

logger = logging.getLogger(__name__)


class RuntimeClient(Protocol):
    async def connect(self, timeout: float = 10.0) -> None: ...
    def events(self) -> AsyncIterator[Any]: ...
    async def execution_started(
        self, assignment: worker_pb2.AssignExecutionRequest
    ) -> None: ...
    async def execution_metrics(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        *,
        elapsed_ms: int | None = None,
        deserialization_ms: int | None = None,
        execution_ms: int | None = None,
        serialization_ms: int | None = None,
        progress: float | None = None,
        custom_metrics: dict[str, float] | None = None,
    ) -> None: ...
    async def execution_completed(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        result: results_pb2.JobResult,
    ) -> None: ...
    async def execution_failed(
        self, assignment: worker_pb2.AssignExecutionRequest, failure: errors_pb2.Error
    ) -> None: ...
    async def close(self) -> None: ...


class PythonRuntime:
    """Execute synchronous ThreadWeave tasks without blocking the session loop."""

    def __init__(
        self, application: BaseThreadWeave[Any], client: RuntimeClient
    ) -> None:
        self.application = application
        self.client = client
        self._executions: dict[str, asyncio.Task[None]] = {}

    async def run_forever(self) -> None:
        await self.client.connect()
        stream_ended = False
        try:
            async for command in self.client.events():
                kind = command.WhichOneof("payload")
                if kind == "assign_execution":
                    assignment = command.assign_execution
                    task = asyncio.create_task(
                        self.execute(assignment),
                        name=f"execution-{assignment.execution_id}",
                    )
                    self._executions[assignment.assignment_id] = task
                    task.add_done_callback(
                        self._execution_done(assignment.assignment_id)
                    )
                elif kind == "cancel_execution":
                    cancellation = command.cancel_execution
                    running_task = self._executions.get(cancellation.assignment_id)
                    if running_task is not None:
                        # Cancelling an asyncio.to_thread await does not stop arbitrary
                        # synchronous code in its OS thread. Process isolation is needed
                        # Forceful cancellation needs process isolation; this only
                        # stops awaiting and reporting the synchronous call.
                        running_task.cancel()
                        logger.info("Execution %s cancelled", cancellation.execution_id)
            stream_ended = True
            await asyncio.gather(*self._executions.values(), return_exceptions=True)
        finally:
            if not stream_ended:
                for task in self._executions.values():
                    task.cancel()
                await asyncio.gather(*self._executions.values(), return_exceptions=True)
            await self.client.close()

    async def execute(self, assignment: worker_pb2.AssignExecutionRequest) -> None:
        await self.client.execution_started(assignment)
        task_id = self._task_id(assignment)
        started = time.perf_counter()
        try:
            deserialize_started = time.perf_counter()
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
            deserialization_ms = _milliseconds(deserialize_started)

            execution_started = time.perf_counter()
            if inspect.iscoroutinefunction(task.__call__):
                value = await task(*args, **kwargs)
            else:
                value = await asyncio.to_thread(task, *args, **kwargs)
            execution_ms = _milliseconds(execution_started)

            serialization_started = time.perf_counter()
            payload = json.dumps(value, separators=(",", ":")).encode()
            serialization_ms = _milliseconds(serialization_started)
            await self.client.execution_metrics(
                assignment,
                elapsed_ms=_milliseconds(started),
                deserialization_ms=deserialization_ms,
                execution_ms=execution_ms,
                serialization_ms=serialization_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.error("Task %s failed: %s: %s", task_id, type(error).__name__, error)
            await self.client.execution_failed(
                assignment,
                errors_pb2.Error(code=type(error).__name__, message=str(error)),
            )
            return
        await self.client.execution_completed(
            assignment,
            results_pb2.JobResult(payload=payload, serialization_format="json"),
        )

    def _execution_done(self, assignment_id: str) -> Any:
        def remove(task: asyncio.Task[None]) -> None:
            if self._executions.get(assignment_id) is task:
                self._executions.pop(assignment_id, None)

        return remove

    def _task_id(self, assignment: worker_pb2.AssignExecutionRequest) -> str:
        if not assignment.HasField("task"):
            raise LookupError("assignment does not contain a task identity")
        identity = assignment.task
        if identity.namespace and identity.application:
            return f"{identity.namespace}.{identity.application}.{identity.name}"
        return identity.name


def _milliseconds(started: float) -> int:
    return round((time.perf_counter() - started) * 1000)


GrpcRuntimeClient = RuntimeProtocolClient

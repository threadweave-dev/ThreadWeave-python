from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import Any

import grpc  # type: ignore[import-untyped]
from google.protobuf.timestamp_pb2 import Timestamp
from threadweave_protocols.common.v1 import errors_pb2
from threadweave_protocols.execution.v1 import results_pb2
from threadweave_protocols.runtime.v1 import heartbeat_pb2, runtime_pb2_grpc, worker_pb2

from threadweave.protocol.common import (
    BaseProtocolClient,
    ProtocolClientError,
    ProtocolUnavailableError,
    grpc_target,
    raise_rpc_error,
)


class RuntimeProtocolClient(BaseProtocolClient):
    """Async facade over one persistent Worker/runtime control session."""

    def __init__(
        self, endpoint: str | None = None, *, runtime_id: str | None = None
    ) -> None:
        super().__init__(endpoint)
        self.runtime_id = runtime_id or str(uuid.uuid4())
        self._channel: grpc.aio.Channel | None = None
        self._call: Any = None
        self._outgoing: asyncio.Queue[Any | None] = asyncio.Queue()
        self._writer_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._sequences: dict[str, int] = {}

    async def connect(self, timeout: float = 10.0) -> None:
        if self._channel is not None:
            return
        channel = grpc.aio.insecure_channel(grpc_target(self._endpoint))
        try:
            await asyncio.wait_for(channel.channel_ready(), timeout)
        except (TimeoutError, grpc.RpcError) as error:
            await channel.close()
            raise ProtocolUnavailableError(
                f"gRPC channel unavailable at {self._endpoint}"
            ) from error
        self._channel = channel
        stub = runtime_pb2_grpc.RuntimeServiceStub(channel)  # type: ignore[no-untyped-call]
        self._call = stub.RuntimeSession()  # type: ignore[attr-defined]
        self._writer_task = asyncio.create_task(self._writer(), name="runtime-events")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeats(), name="runtime-heartbeats"
        )
        await self._send(
            worker_pb2.RuntimeEvent(  # type: ignore[attr-defined]
                ready=worker_pb2.RuntimeReady(  # type: ignore[attr-defined]
                    runtime_id=self.runtime_id
                )
            )
        )

    async def events(self) -> AsyncIterator[Any]:
        call = self._require_call()
        try:
            async for command in call:
                yield command
        except grpc.RpcError as error:
            raise_rpc_error(error, "RuntimeSession")

    async def execution_started(
        self, assignment: worker_pb2.AssignExecutionRequest
    ) -> None:
        await self._send_event(
            assignment,
            "execution_started",
            worker_pb2.ExecutionStarted,  # type: ignore[attr-defined]
            observed_at=_now(),
        )

    async def execution_progress(
        self, assignment: worker_pb2.AssignExecutionRequest, progress: float
    ) -> None:
        await self._send_event(
            assignment,
            "execution_progress",
            worker_pb2.ExecutionProgress,  # type: ignore[attr-defined]
            progress=progress,
            observed_at=_now(),
        )

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
    ) -> None:
        values: dict[str, Any] = {"custom_metrics": custom_metrics or {}}
        for name, value in (
            ("elapsed_ms", elapsed_ms),
            ("deserialization_ms", deserialization_ms),
            ("execution_ms", execution_ms),
            ("serialization_ms", serialization_ms),
            ("progress", progress),
        ):
            if value is not None:
                values[name] = value
        await self._send_event(
            assignment,
            "execution_metrics",
            worker_pb2.ExecutionMetrics,  # type: ignore[attr-defined]
            **values,
        )

    async def execution_completed(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        result: results_pb2.JobResult,
    ) -> None:
        await self._send_event(
            assignment,
            "execution_completed",
            worker_pb2.ExecutionCompleted,  # type: ignore[attr-defined]
            result=result,
            observed_at=_now(),
        )
        self._sequences.pop(assignment.assignment_id, None)

    async def execution_failed(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        failure: errors_pb2.Error,
    ) -> None:
        await self._send_event(
            assignment,
            "execution_failed",
            worker_pb2.ExecutionFailed,  # type: ignore[attr-defined]
            failure=failure,
            observed_at=_now(),
        )
        self._sequences.pop(assignment.assignment_id, None)

    async def _send_event(
        self,
        assignment: worker_pb2.AssignExecutionRequest,
        field: str,
        event_type: Any,
        **values: Any,
    ) -> None:
        sequence = self._sequences.get(assignment.assignment_id, 0) + 1
        self._sequences[assignment.assignment_id] = sequence
        payload = event_type(
            assignment_id=assignment.assignment_id,
            execution_id=assignment.execution_id,
            sequence_number=sequence,
            **values,
        )
        await self._send(
            worker_pb2.RuntimeEvent(**{field: payload})  # type: ignore[attr-defined]
        )

    async def _send(self, event: Any) -> None:
        self._require_call()
        await self._outgoing.put(event)

    async def _writer(self) -> None:
        call = self._require_call()
        while (event := await self._outgoing.get()) is not None:
            await call.write(event)
        await call.done_writing()

    async def _heartbeats(self) -> None:
        sequence = 0
        try:
            while True:
                await asyncio.sleep(10)
                sequence += 1
                await self._send(
                    worker_pb2.RuntimeEvent(  # type: ignore[attr-defined]
                        heartbeat=heartbeat_pb2.RuntimeHeartbeat(
                            runtime_id=self.runtime_id,
                            sequence_number=sequence,
                            observed_at=_now(),
                        )
                    )
                )
        except asyncio.CancelledError:
            raise

    def _require_call(self) -> Any:
        if self._call is None:
            raise ProtocolClientError("runtime protocol client is not connected")
        return self._call

    async def close(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        if self._writer_task is not None:
            await self._outgoing.put(None)
            with contextlib.suppress(grpc.RpcError):
                await self._writer_task
        if self._channel is not None:
            await self._channel.close()
        self._channel = None
        self._call = None
        self._writer_task = None
        self._heartbeat_task = None

    async def __aenter__(self) -> RuntimeProtocolClient:
        await self.connect()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()


def _now() -> Timestamp:
    value = Timestamp()
    value.GetCurrentTime()
    return value


GrpcRuntimeClient = RuntimeProtocolClient

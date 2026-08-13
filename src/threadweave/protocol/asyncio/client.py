from __future__ import annotations

import asyncio

import grpc  # type: ignore[import-untyped]
from threadweave_protocols.execution.v1 import execution_pb2, execution_pb2_grpc

from threadweave.protocol.common import (
    BaseProtocolClient,
    GetJobResult,
    ProtocolClientError,
    ProtocolTimeoutError,
    ProtocolUnavailableError,
    SubmitJobResult,
    build_submit_job_request,
    grpc_target,
    parse_get_job_response,
    parse_submit_job_response,
    raise_rpc_error,
)

# Compatibility names retained for users of the original gRPC POC.
GrpcClientError = ProtocolClientError
GrpcUnavailableError = ProtocolUnavailableError
GrpcTimeoutError = ProtocolTimeoutError


class AsyncGrpcClient(BaseProtocolClient):
    def __init__(
        self,
        endpoint: str | None = None,
        *,
        namespace: str | None = None,
        application: str | None = None,
    ) -> None:
        super().__init__(
            endpoint,
            namespace=namespace,
            application=application,
        )
        self._channel: grpc.aio.Channel | None = None
        self._stub: execution_pb2_grpc.ExecutionServiceStub | None = None

    async def connect(self, timeout: float = 10.0) -> None:
        if self._channel is not None:
            return

        channel = grpc.aio.insecure_channel(grpc_target(self._endpoint))
        try:
            await asyncio.wait_for(channel.channel_ready(), timeout=timeout)
        except (TimeoutError, grpc.aio.AioRpcError) as error:
            await channel.close()
            raise GrpcUnavailableError(
                f"gRPC channel unavailable at {self._endpoint}"
            ) from error

        self._channel = channel
        self._stub = execution_pb2_grpc.ExecutionServiceStub(  # type: ignore[no-untyped-call]
            channel
        )

    async def submit_job(
        self,
        *,
        namespace: str,
        application: str,
        task: str,
        task_version: str | None = None,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        metadata: dict[str, str] | None = None,
        timeout: float = 10.0,
    ) -> SubmitJobResult:
        if self._stub is None:
            raise GrpcClientError("gRPC client is not connected")

        request = build_submit_job_request(
            namespace=namespace,
            application=application,
            task=task,
            task_version=task_version,
            args=args,
            kwargs=kwargs,
            metadata=metadata,
        )
        try:
            response = await self._stub.SubmitTask(request, timeout=timeout)
        except grpc.aio.AioRpcError as error:
            raise_rpc_error(error, "SubmitTask")

        return parse_submit_job_response(response)

    async def get_job(
        self, job_id: str, *, timeout: float | None = None
    ) -> GetJobResult:
        if self._stub is None:
            raise GrpcClientError("gRPC client is not connected")

        try:
            response = await self._stub.GetJob(
                execution_pb2.GetJobRequest(job_id=job_id), timeout=timeout
            )
        except grpc.aio.AioRpcError as error:
            raise_rpc_error(error, "GetJob")
        return parse_get_job_response(response)

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
        self._channel = None
        self._stub = None

    async def __aenter__(self) -> AsyncGrpcClient:
        await self.connect()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()


AsyncProtocolClient = AsyncGrpcClient

from __future__ import annotations

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


class GrpcClient(BaseProtocolClient):
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
        self._channel: grpc.Channel | None = None
        self._stub: execution_pb2_grpc.ExecutionServiceStub | None = None

    def connect(self, timeout: float = 10.0) -> None:
        if self._channel is not None:
            return

        channel = grpc.insecure_channel(grpc_target(self._endpoint))
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
        except grpc.FutureTimeoutError as error:
            channel.close()
            raise GrpcUnavailableError(
                f"gRPC channel unavailable at {self._endpoint}"
            ) from error

        self._channel = channel
        self._stub = execution_pb2_grpc.ExecutionServiceStub(  # type: ignore[no-untyped-call]
            channel
        )

    def submit_job(
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
            response = self._stub.SubmitTask(request, timeout=timeout)
        except grpc.RpcError as error:
            raise_rpc_error(error, "SubmitTask")

        return parse_submit_job_response(response)

    def get_job(
        self, job_id: str, *, timeout: float | None = None
    ) -> GetJobResult:
        if self._stub is None:
            raise GrpcClientError("gRPC client is not connected")

        try:
            response = self._stub.GetJob(
                execution_pb2.GetJobRequest(job_id=job_id), timeout=timeout
            )
        except grpc.RpcError as error:
            raise_rpc_error(error, "GetJob")
        return parse_get_job_response(response)

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()
        self._channel = None
        self._stub = None

    def __enter__(self) -> GrpcClient:
        self.connect()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


ProtocolClient = GrpcClient

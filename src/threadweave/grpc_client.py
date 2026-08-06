from __future__ import annotations

import json
from dataclasses import dataclass

import grpc
from threadweave_protocols.common.v1 import metadata_pb2
from threadweave_protocols.execution.v1 import (
    execution_pb2,
    execution_pb2_grpc,
    jobs_pb2,
)


class GrpcClientError(RuntimeError):
    """Base error raised by the synchronous gRPC POC client."""


class GrpcUnavailableError(GrpcClientError):
    """Raised when the Core channel does not become ready."""


class GrpcTimeoutError(GrpcClientError):
    """Raised when a gRPC request reaches its deadline."""


@dataclass(frozen=True, slots=True)
class SubmitJobResult:
    job_id: str
    state: str


class GrpcClient:
    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._channel: grpc.Channel | None = None
        self._stub: execution_pb2_grpc.ExecutionServiceStub | None = None

    def connect(self, timeout: float = 10.0) -> None:
        if self._channel is not None:
            return
        target = self._endpoint.removeprefix("http://")
        channel = grpc.insecure_channel(target)
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
        except grpc.FutureTimeoutError as error:
            channel.close()
            raise GrpcUnavailableError(
                f"gRPC channel unavailable at {self._endpoint}"
            ) from error
        self._channel = channel
        self._stub = execution_pb2_grpc.ExecutionServiceStub(channel)

    def submit_job(
        self,
        *,
        namespace: str,
        application: str,
        task: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        metadata: dict[str, str] | None = None,
        timeout: float = 10.0,
    ) -> SubmitJobResult:
        if self._stub is None:
            raise GrpcClientError("gRPC client is not connected")

        entries = dict(metadata or {})
        entries["application"] = application
        request = execution_pb2.SubmitTaskRequest(
            application_namespace=namespace,
            task_name=task,
            arguments=json.dumps(
                {"args": args, "kwargs": kwargs},
                separators=(",", ":"),
            ).encode(),
            serialization_format="json",
            metadata=metadata_pb2.Metadata(entries=entries),
        )
        try:
            response = self._stub.SubmitTask(request, timeout=timeout)
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise GrpcTimeoutError("SubmitTask deadline exceeded") from error
            raise GrpcClientError(
                f"SubmitTask failed: {error.code().name}: {error.details()}"
            ) from error

        if not response.HasField("job") or not response.job.job_id:
            raise GrpcClientError("SubmitTask response does not contain a job_id")
        state_name = (
            "JOB_STATE_ACCEPTED"
            if response.job.state == 7
            else jobs_pb2.JobState.Name(response.job.state)
        )
        return SubmitJobResult(
            job_id=response.job.job_id,
            state=state_name.removeprefix("JOB_STATE_"),
        )

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

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import NoReturn

import grpc  # type: ignore[import-untyped]
from threadweave_protocols.common.v1 import metadata_pb2
from threadweave_protocols.execution.v1 import execution_pb2, jobs_pb2

DEFAULT_ENDPOINT = "unix:///tmp/threadweave.sock"
_JOB_STATE_ACCEPTED = jobs_pb2.JOB_STATE_ACCEPTED


class ProtocolClientError(RuntimeError):
    """Base error raised by a ThreadWeave protocol client."""


class ProtocolUnavailableError(ProtocolClientError):
    """Raised when the Core channel does not become ready."""


class ProtocolTimeoutError(ProtocolClientError):
    """Raised when a protocol request reaches its deadline."""


@dataclass(frozen=True, slots=True)
class SubmitJobResult:
    """Minimal job representation returned after a submission."""

    job_id: str
    state: str


@dataclass(frozen=True, slots=True)
class GetJobResult:
    """Current job state and its result payload, when available."""

    state: str
    payload: bytes | None
    serialization_format: str | None
    failure: str | None


class BaseProtocolClient:
    """Configuration shared by blocking and asyncio protocol clients."""

    def __init__(
        self,
        endpoint: str | None = None,
        *,
        namespace: str | None = None,
        application: str | None = None,
    ) -> None:
        self._endpoint = endpoint or DEFAULT_ENDPOINT
        self._namespace = namespace
        self._application = application

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def namespace(self) -> str | None:
        return self._namespace

    @property
    def application(self) -> str | None:
        return self._application


def grpc_target(endpoint: str) -> str:
    """Return the target syntax expected by gRPC."""
    return endpoint.removeprefix("http://")


def build_submit_job_request(
    *,
    namespace: str,
    application: str,
    task: str,
    task_version: str | None = None,
    args: tuple[object, ...],
    kwargs: dict[str, object],
    metadata: dict[str, str] | None = None,
) -> execution_pb2.SubmitTaskRequest:
    """Build the wire request shared by blocking and asyncio clients."""
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
        command_id=str(uuid.uuid4()),
    )
    if task_version is not None:
        request.task.namespace = namespace
        request.task.application = application
        request.task.name = task
        request.task.version = task_version
    return request


def parse_submit_job_response(
    response: execution_pb2.SubmitTaskResponse,
) -> SubmitJobResult:
    """Validate and convert a SubmitTask response to the public result."""
    if not response.HasField("job") or not response.job.job_id:
        raise ProtocolClientError("SubmitTask response does not contain a job_id")

    if response.job.state == _JOB_STATE_ACCEPTED:
        state = "JOB_STATE_ACCEPTED"
    else:
        try:
            state = jobs_pb2.JobState.Name(response.job.state)
        except ValueError as error:
            raise ProtocolClientError(
                f"SubmitTask response contains unknown job state {response.job.state}"
            ) from error

    return SubmitJobResult(
        job_id=response.job.job_id,
        state=state.removeprefix("JOB_STATE_"),
    )


def parse_get_job_response(response: execution_pb2.GetJobResponse) -> GetJobResult:
    """Validate and convert a GetJob response."""
    if not response.HasField("job"):
        raise ProtocolClientError("GetJob response does not contain a job")

    try:
        state = jobs_pb2.JobState.Name(response.job.state)
    except ValueError as error:
        raise ProtocolClientError(
            f"GetJob response contains unknown job state {response.job.state}"
        ) from error

    if not response.HasField("result"):
        return GetJobResult(state.removeprefix("JOB_STATE_"), None, None, None)

    result = response.result
    failure = str(result.failure) if result.HasField("failure") else None
    return GetJobResult(
        state.removeprefix("JOB_STATE_"),
        result.payload,
        result.serialization_format or None,
        failure,
    )


def raise_rpc_error(error: grpc.RpcError, operation: str) -> NoReturn:
    """Translate a gRPC exception into the shared client error hierarchy."""
    code = error.code()
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        raise ProtocolTimeoutError(f"{operation} deadline exceeded") from error

    code_name = code.name if code is not None else "UNKNOWN"
    raise ProtocolClientError(
        f"{operation} failed: {code_name}: {error.details()}"
    ) from error

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4


DEFAULT_ENDPOINT = "unix:///tmp/threadweave.sock"
DEFAULT_REQUEST_TIMEOUT = 30.0


class ProtocolClientError(RuntimeError):
    """Base exception raised by the ThreadWeave protocol client."""


class ClientNotConnectedError(ProtocolClientError):
    """Raised when an operation requires an active transport connection."""


class ProtocolRequestError(ProtocolClientError):
    """
    Raised when the Core rejects a protocol request.

    Attributes
    ----------
    code:
        Stable language-neutral error code returned by the Core.
    details:
        Optional structured error details.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "protocol_error",
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


class ProtocolResponseError(ProtocolClientError):
    """Raised when a response does not satisfy the expected protocol shape."""


class ProtocolTimeoutError(ProtocolClientError):
    """Raised when a protocol request exceeds its local request timeout."""


@dataclass(frozen=True, slots=True)
class SubmitJobResponse:
    """Response returned after the Core accepts a Job submission."""

    job_id: str

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> SubmitJobResponse:
        job_id = payload.get("job_id")

        if not isinstance(job_id, str) or not job_id:
            raise ProtocolResponseError(
                "Submit Job response is missing a valid 'job_id'."
            )

        return cls(job_id=job_id)


@dataclass(frozen=True, slots=True)
class WaitForJobResultResponse:
    """Terminal Job result returned by the Core."""

    state: str
    result: Any = None
    error: Mapping[str, Any] | None = None
    has_result: bool = True

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> WaitForJobResultResponse:
        state = payload.get("state")

        if not isinstance(state, str) or not state:
            raise ProtocolResponseError(
                "Job result response is missing a valid 'state'."
            )

        error = payload.get("error")

        if error is not None and not isinstance(error, Mapping):
            raise ProtocolResponseError(
                "Job result 'error' must be an object or null."
            )

        return cls(
            state=state,
            result=payload.get("result"),
            error=dict(error) if error is not None else None,
            has_result=bool(payload.get("has_result", "result" in payload)),
        )


@dataclass(frozen=True, slots=True)
class CancelJobResponse:
    """Response returned after requesting Job cancellation."""

    accepted: bool

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> CancelJobResponse:
        accepted = payload.get("accepted")

        if not isinstance(accepted, bool):
            raise ProtocolResponseError(
                "Cancel Job response is missing a boolean 'accepted' field."
            )

        return cls(accepted=accepted)


@runtime_checkable
class ProtocolTransport(Protocol):
    """
    Transport interface used by ProtocolClient.

    A transport implementation is responsible for connection management,
    framing, encoding and physical communication. It must not implement
    ThreadWeave domain behavior.
    """

    @property
    def is_connected(self) -> bool:
        """Return whether the transport currently has an active connection."""
        ...

    async def connect(self) -> None:
        """Open the underlying transport connection."""
        ...

    async def close(self) -> None:
        """Close the underlying transport connection."""
        ...

    async def request(
        self,
        message: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Send one request and return its response."""
        ...

    def stream(
        self,
        message: Mapping[str, Any],
    ) -> AsyncIterator[Mapping[str, Any]]:
        """Open a server-to-client event stream."""
        ...


class ProtocolClient:
    """
    High-level client for communication with the ThreadWeave Core.

    This class owns protocol operations such as Job submission, status lookup,
    cancellation and result waiting. Physical transport is delegated to a
    ProtocolTransport implementation.

    Parameters
    ----------
    endpoint:
        Core endpoint, for example ``unix:///tmp/threadweave.sock``.
    namespace:
        ThreadWeave namespace used by the application.
    application:
        Application identifier within the namespace.
    transport:
        Optional transport implementation. A default transport factory can be
        introduced once the concrete wire transport is finalized.
    request_timeout:
        Default local timeout for individual protocol requests.
    """

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        namespace: str,
        application: str,
        transport: ProtocolTransport | None = None,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        if not namespace:
            raise ValueError("namespace cannot be empty.")

        if not application:
            raise ValueError("application cannot be empty.")

        if isinstance(request_timeout, bool) or not isinstance(
            request_timeout,
            (int, float),
        ):
            raise TypeError("request_timeout must be a number of seconds.")

        if request_timeout <= 0:
            raise ValueError("request_timeout must be greater than zero.")

        self._endpoint = endpoint or DEFAULT_ENDPOINT
        self._namespace = namespace
        self._application = application
        self._request_timeout = float(request_timeout)
        self._transport = transport

        self._connect_lock = asyncio.Lock()

    @property
    def endpoint(self) -> str:
        """Return the configured Core endpoint."""
        return self._endpoint

    @property
    def namespace(self) -> str:
        """Return the configured namespace."""
        return self._namespace

    @property
    def application(self) -> str:
        """Return the configured application name."""
        return self._application

    @property
    def is_connected(self) -> bool:
        """Return whether the underlying transport is connected."""
        return (
            self._transport is not None
            and self._transport.is_connected
        )

    async def connect(self) -> None:
        """
        Connect to the ThreadWeave Core.

        Multiple concurrent calls are serialized and connecting an already
        connected client is a no-op.
        """

        async with self._connect_lock:
            if self.is_connected:
                return

            transport = self._require_transport()
            await transport.connect()

    async def close(self) -> None:
        """Close the transport connection."""

        if self._transport is None:
            return

        if not self._transport.is_connected:
            return

        await self._transport.close()

    async def submit_job(
        self,
        *,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        options: Mapping[str, Any],
        metadata: Mapping[str, Any],
    ) -> SubmitJobResponse:
        """
        Submit a Task invocation to the ThreadWeave Core.

        Serialization is intentionally left to the transport or protocol codec.
        This method builds the language-neutral protocol message.
        """

        if not task_id:
            raise ValueError("task_id cannot be empty.")

        payload = {
            "task_id": task_id,
            "arguments": {
                "args": list(args),
                "kwargs": dict(kwargs),
            },
            "options": dict(options),
            "metadata": dict(metadata),
        }

        response = await self._request(
            operation="job.submit",
            payload=payload,
        )

        return SubmitJobResponse.from_payload(response)

    async def get_job(
        self,
        job_id: str,
    ) -> Mapping[str, Any]:
        """Fetch the latest snapshot of a Job."""

        self._validate_job_id(job_id)

        return await self._request(
            operation="job.get",
            payload={"job_id": job_id},
        )

    async def wait_for_job(
        self,
        job_id: str,
    ) -> Mapping[str, Any]:
        """
        Wait until a Job reaches a terminal state.

        The server controls the duration of this protocol operation. A future
        implementation may use a stream internally instead of one long-lived
        request.
        """

        self._validate_job_id(job_id)

        return await self._request(
            operation="job.wait",
            payload={"job_id": job_id},
            timeout=None,
        )

    async def wait_for_job_result(
        self,
        job_id: str,
    ) -> WaitForJobResultResponse:
        """Wait for a Job and retrieve its terminal result."""

        self._validate_job_id(job_id)

        response = await self._request(
            operation="job.result",
            payload={"job_id": job_id},
            timeout=None,
        )

        return WaitForJobResultResponse.from_payload(response)

    async def cancel_job(
        self,
        job_id: str,
    ) -> CancelJobResponse:
        """Request cancellation of a Job."""

        self._validate_job_id(job_id)

        response = await self._request(
            operation="job.cancel",
            payload={"job_id": job_id},
        )

        return CancelJobResponse.from_payload(response)

    async def watch_job(
        self,
        job_id: str,
    ) -> AsyncIterator[Mapping[str, Any]]:
        """
        Stream events related to one Job.

        The returned event payloads remain protocol-level mappings until the
        canonical typed event model is finalized.
        """

        self._validate_job_id(job_id)
        self._ensure_connected()

        request = self._build_message(
            operation="job.watch",
            payload={"job_id": job_id},
        )

        transport = self._require_transport()

        try:
            async for message in transport.stream(request):
                yield self._unwrap_stream_message(message)
        except asyncio.CancelledError:
            raise
        except ProtocolClientError:
            raise
        except Exception as exc:
            raise ProtocolClientError(
                f"Job event stream failed: {exc}"
            ) from exc

    async def health(self) -> Mapping[str, Any]:
        """Query the Core health endpoint."""

        return await self._request(
            operation="system.health",
            payload={},
        )

    async def _request(
        self,
        *,
        operation: str,
        payload: Mapping[str, Any],
        timeout: float | None | object = ...,
    ) -> Mapping[str, Any]:
        self._ensure_connected()

        message = self._build_message(
            operation=operation,
            payload=payload,
        )

        transport = self._require_transport()

        if timeout is ...:
            effective_timeout: float | None = self._request_timeout
        else:
            effective_timeout = timeout

        try:
            if effective_timeout is None:
                response = await transport.request(message)
            else:
                async with asyncio.timeout(effective_timeout):
                    response = await transport.request(message)
        except TimeoutError as exc:
            raise ProtocolTimeoutError(
                f"Protocol operation {operation!r} timed out."
            ) from exc
        except asyncio.CancelledError:
            raise
        except ProtocolClientError:
            raise
        except Exception as exc:
            raise ProtocolClientError(
                f"Protocol operation {operation!r} failed: {exc}"
            ) from exc

        return self._unwrap_response(
            response,
            expected_request_id=str(message["request_id"]),
        )

    def _build_message(
        self,
        *,
        operation: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "protocol": "threadweave",
            "version": "1",
            "kind": "request",
            "request_id": str(uuid4()),
            "operation": operation,
            "context": {
                "namespace": self._namespace,
                "application": self._application,
            },
            "payload": dict(payload),
        }

    def _unwrap_response(
        self,
        response: Mapping[str, Any],
        *,
        expected_request_id: str,
    ) -> Mapping[str, Any]:
        if not isinstance(response, Mapping):
            raise ProtocolResponseError(
                "The transport returned a non-object response."
            )

        request_id = response.get("request_id")

        if request_id != expected_request_id:
            raise ProtocolResponseError(
                "The response request_id does not match the request."
            )

        kind = response.get("kind")

        if kind != "response":
            raise ProtocolResponseError(
                f"Expected response kind 'response', received {kind!r}."
            )

        success = response.get("success")

        if not isinstance(success, bool):
            raise ProtocolResponseError(
                "The response is missing a boolean 'success' field."
            )

        if not success:
            error = response.get("error")

            if not isinstance(error, Mapping):
                raise ProtocolRequestError(
                    "The Core rejected the request without error details."
                )

            raise ProtocolRequestError(
                str(error.get("message", "The Core rejected the request.")),
                code=str(error.get("code", "protocol_error")),
                details=(
                    error.get("details")
                    if isinstance(error.get("details"), Mapping)
                    else None
                ),
            )

        payload = response.get("payload")

        if payload is None:
            return {}

        if not isinstance(payload, Mapping):
            raise ProtocolResponseError(
                "The response payload must be an object."
            )

        return payload

    def _unwrap_stream_message(
        self,
        message: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if not isinstance(message, Mapping):
            raise ProtocolResponseError(
                "The stream returned a non-object message."
            )

        kind = message.get("kind")

        if kind == "error":
            error = message.get("error")

            if not isinstance(error, Mapping):
                raise ProtocolRequestError(
                    "The event stream failed without error details."
                )

            raise ProtocolRequestError(
                str(error.get("message", "The event stream failed.")),
                code=str(error.get("code", "stream_error")),
                details=(
                    error.get("details")
                    if isinstance(error.get("details"), Mapping)
                    else None
                ),
            )

        if kind != "event":
            raise ProtocolResponseError(
                f"Expected stream message kind 'event', received {kind!r}."
            )

        payload = message.get("payload")

        if not isinstance(payload, Mapping):
            raise ProtocolResponseError(
                "The event payload must be an object."
            )

        return payload

    def _ensure_connected(self) -> None:
        if not self.is_connected:
            raise ClientNotConnectedError(
                "ProtocolClient is not connected. "
                "Call 'await client.connect()' before performing requests."
            )

    def _require_transport(self) -> ProtocolTransport:
        if self._transport is None:
            raise ProtocolClientError(
                "No protocol transport is configured. Pass a transport "
                "implementation to ProtocolClient."
            )

        return self._transport

    @staticmethod
    def _validate_job_id(job_id: str) -> None:
        if not isinstance(job_id, str):
            raise TypeError("job_id must be a string.")

        if not job_id.strip():
            raise ValueError("job_id cannot be empty.")

    async def __aenter__(self) -> ProtocolClient:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        await self.close()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"endpoint={self._endpoint!r}, "
            f"namespace={self._namespace!r}, "
            f"application={self._application!r}, "
            f"connected={self.is_connected!r}"
            f")"
        )

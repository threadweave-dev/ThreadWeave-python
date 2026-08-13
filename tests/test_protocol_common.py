from __future__ import annotations

import json

import pytest
from threadweave_protocols.execution.v1 import execution_pb2

from threadweave.protocol.common import (
    ProtocolClientError,
    build_submit_job_request,
    parse_submit_job_response,
)


def test_build_submit_job_request() -> None:
    request = build_submit_job_request(
        namespace="development",
        application="demo",
        task="demo.add",
        task_version="2026.08.12",
        args=(1, 2),
        kwargs={"trace": True},
        metadata={"application": "overridden", "caller": "test"},
    )

    assert request.application_namespace == "development"
    assert request.task_name == "demo.add"
    assert json.loads(request.arguments) == {
        "args": [1, 2],
        "kwargs": {"trace": True},
    }
    assert request.serialization_format == "json"
    assert request.command_id
    assert request.task.namespace == "development"
    assert request.task.application == "demo"
    assert request.task.name == "demo.add"
    assert request.task.version == "2026.08.12"
    assert request.metadata.entries == {
        "application": "demo",
        "caller": "test",
    }


def test_parse_submit_job_response() -> None:
    response = execution_pb2.SubmitTaskResponse()
    response.job.job_id = "job-1"
    response.job.state = 7  # type: ignore[assignment]

    assert parse_submit_job_response(response).job_id == "job-1"
    assert parse_submit_job_response(response).state == "ACCEPTED"


def test_parse_submit_job_response_requires_job_id() -> None:
    with pytest.raises(ProtocolClientError, match="job_id"):
        parse_submit_job_response(execution_pb2.SubmitTaskResponse())

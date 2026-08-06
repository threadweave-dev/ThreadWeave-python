from __future__ import annotations

import os
from pathlib import Path

import pytest

from threadweave.core_process import CoreProcess, CoreProcessError, decode_ready_message
from threadweave.grpc_client import GrpcClient


def test_decode_ready_message() -> None:
    endpoint = decode_ready_message(
        '{"type":"ready","endpoint":"http://127.0.0.1:1234",'
        '"transport":"tcp","protocol":"grpc"}'
    )
    assert endpoint.address == "http://127.0.0.1:1234"


def test_rejects_invalid_json() -> None:
    with pytest.raises(CoreProcessError, match="invalid ready JSON"):
        decode_ready_message("{")


def test_rejects_missing_endpoint() -> None:
    with pytest.raises(CoreProcessError, match="missing an endpoint"):
        decode_ready_message('{"type":"ready","transport":"tcp","protocol":"grpc"}')


@pytest.mark.skipif(
    "THREADWEAVE_CORE_EXECUTABLE" not in os.environ,
    reason="set THREADWEAVE_CORE_EXECUTABLE to run the Rust/Python E2E test",
)
def test_synchronous_grpc_end_to_end() -> None:
    executable = Path(os.environ["THREADWEAVE_CORE_EXECUTABLE"])
    core = CoreProcess(executable)
    endpoint = core.start()
    try:
        with GrpcClient(endpoint.address) as client:
            result = client.submit_job(
                namespace="development",
                application="demo",
                task="demo.add",
                args=(1, 2),
                kwargs={},
            )
        assert result.job_id
        assert result.state == "ACCEPTED"
    finally:
        core.stop()
    assert core.endpoint is None

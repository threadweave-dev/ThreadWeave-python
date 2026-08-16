from pathlib import Path

import pytest
from typer.testing import CliRunner

from threadweave.cli.main import app as cli_app
from threadweave.cli.worker import load_application


def test_load_application_imports_module_from_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application_module = tmp_path / "example_application.py"
    application_module.write_text(
        "from threadweave import ThreadWeave\n\n"
        'tw = ThreadWeave("example")\n\n'
        "@tw.task\n"
        "def add(a: int, b: int) -> int:\n"
        "    return a + b\n"
    )
    monkeypatch.chdir(tmp_path)

    application = load_application("example_application:tw")

    assert application.qualified_name == "default/example"
    assert [task.name for task in application.discover_tasks()] == ["add"]


def test_worker_address_is_passed_to_runtime_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO")
    application_module = tmp_path / "example_application.py"
    application_module.write_text(
        "from threadweave import ThreadWeave\n"
        'tw = ThreadWeave("example", grpc_address="localhost:50051")\n'
    )
    monkeypatch.chdir(tmp_path)
    endpoints: list[str] = []

    class FakeRuntimeClient:
        def __init__(self, endpoint: str) -> None:
            endpoints.append(endpoint)

        def connect(self) -> None:
            pass

    class FakePythonRuntime:
        def __init__(self, application: object, client: object) -> None:
            pass

        def run_forever(self) -> None:
            raise KeyboardInterrupt

    monkeypatch.setattr(
        "threadweave.cli.worker.RuntimeProtocolClient", FakeRuntimeClient
    )
    monkeypatch.setattr(
        "threadweave.cli.worker.PythonRuntime", FakePythonRuntime
    )

    result = CliRunner().invoke(
        cli_app,
        ["worker", "example_application:tw", "--worker-addr", "127.0.0.1:50052"],
    )

    assert result.exit_code == 0
    assert endpoints == ["127.0.0.1:50052"]
    assert "Connected to Rust Worker at 127.0.0.1:50052" in caplog.text

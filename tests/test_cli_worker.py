from pathlib import Path

import pytest

from threadweave.cli.worker import load_application


def test_load_application_imports_module_from_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application_module = tmp_path / "example_application.py"
    application_module.write_text(
        "from threadweave import ThreadWeave\n\n"
        'tw = ThreadWeave("example")\n'
    )
    monkeypatch.chdir(tmp_path)

    application = load_application("example_application:tw")

    assert application.qualified_name == "default/example"

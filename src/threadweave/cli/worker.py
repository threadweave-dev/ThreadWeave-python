from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Annotated

import typer

from threadweave.app import ThreadWeave
from threadweave.runtime.worker import GrpcRuntimeClient, Worker

app = typer.Typer(
    help="Run a ThreadWeave worker application.",
    no_args_is_help=True,
)


def load_application(import_string: str) -> ThreadWeave:
    """Load a ThreadWeave application from ``module:attribute``."""
    module_name, separator, attribute_name = import_string.partition(":")
    if not separator or not module_name or not attribute_name:
        raise typer.BadParameter("application must use the form 'module:attribute'")

    working_directory = str(Path.cwd())
    if working_directory not in sys.path:
        sys.path.insert(0, working_directory)

    try:
        module = importlib.import_module(module_name)
        application = getattr(module, attribute_name)
    except (ImportError, AttributeError) as error:
        raise typer.BadParameter(
            f"could not load application {import_string!r}: {error}"
        ) from error

    if not isinstance(application, ThreadWeave):
        raise typer.BadParameter(
            f"{import_string!r} does not refer to a ThreadWeave application"
        )
    return application


@app.command()
def run(
    application: Annotated[
        str,
        typer.Argument(help="Application to load, in module:attribute form."),
    ],
) -> None:
    """Load an application and execute tasks assigned by the Core."""
    worker_application = load_application(application)
    typer.echo(f"Loaded {worker_application.qualified_name}")
    runtime_client = GrpcRuntimeClient(worker_application.client.endpoint)
    worker = Worker(worker_application, runtime_client)
    try:
        worker.run_forever()
    except KeyboardInterrupt:
        typer.echo("Worker stopped")


def main() -> None:
    app()


if __name__ == "__main__":
    main()

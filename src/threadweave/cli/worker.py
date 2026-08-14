from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Annotated, Any

import typer

from threadweave._internal.app import BaseThreadWeave

app = typer.Typer(
    help="Run a ThreadWeave worker application.",
    no_args_is_help=True,
)


def load_application(import_string: str) -> BaseThreadWeave[Any]:
    """Load a ThreadWeave application from ``module:attribute``."""
    module_name, separator, attribute_name = import_string.partition(":")
    if not separator or not module_name or not attribute_name:
        raise typer.BadParameter(
            "application must use the form 'module:attribute'"
        )

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

    if not isinstance(application, BaseThreadWeave):
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
    """Load an application and display the tasks available to the worker."""
    worker_application = load_application(application)
    typer.echo(f"Loaded {worker_application.qualified_name}")
    for task in worker_application.discover_tasks():
        typer.echo(task.id)


def main() -> None:
    app()


if __name__ == "__main__":
    main()

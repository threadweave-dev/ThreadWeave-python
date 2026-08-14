from __future__ import annotations

import typer

from threadweave.cli.worker import run

app = typer.Typer(help="ThreadWeave command-line interface.", no_args_is_help=True)
app.command("worker")(run)


@app.callback()
def cli() -> None:
    """Manage ThreadWeave applications and workers."""


def main() -> None:
    app()


if __name__ == "__main__":
    main()

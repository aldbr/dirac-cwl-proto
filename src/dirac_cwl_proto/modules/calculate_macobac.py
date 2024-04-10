#!/usr/bin/env python3

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def calculate_macobac(
    config: str = typer.Option(..., help="Configuration in yaml format"),
    log_level: str = typer.Option(..., help="Log level"),
):
    """
    Fake calculate macobac.
    """
    # Read the configuration and print it
    with open(config) as f:
        config_data = f.read()

    console.print(f"Macobac: [bold green]{config_data}[/bold green]")
    console.print("[bold yellow]Macobac completed[/bold yellow]")


if __name__ == "__main__":
    app()

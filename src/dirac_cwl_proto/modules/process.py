#!/usr/bin/env python3

from typing import List

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def process(files: List[str] = typer.Argument(..., help="Paths to the input files")):
    """
    Process the input files.
    """
    # Processing the input files
    final_output = []
    for file in files:
        console.print(f"Processing file: [bold green]{file}[/bold green]")
        final_output.append("processed_" + file)

    with open("output.dst", "w") as f:
        f.write("\n".join(final_output))

    console.print("[bold yellow]Processing completed[/bold yellow]")


if __name__ == "__main__":
    app()

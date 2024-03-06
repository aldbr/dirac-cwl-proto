#!/usr/bin/env python3
import random

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def simulate(
    max_random: int = typer.Argument(..., help="Maximum random number"),
    min_random: int = typer.Argument(..., help="Minimum random number"),
):
    """
    Simulate a random number generation.
    """
    # Generate a random result for simulation
    result_sim = random.randint(min_random, max_random)

    # Displaying results on the console
    console.print(f"Simulation Result: [bold green]{result_sim}[/bold green]")

    # Writing outputs to files
    output_path = "result.sim"
    with open(output_path, "w") as file:
        file.write(str(result_sim))

    return output_path


if __name__ == "__main__":
    app()

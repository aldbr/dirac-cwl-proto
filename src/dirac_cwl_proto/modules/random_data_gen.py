import random

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def generate_random_data(file_path: str = "data.txt", num_lines: int = 100):
    with open(file_path, "w") as f:
        mu = random.randint(1, 10)
        sig = random.randint(1, 5)
        for _ in range(num_lines):
            rd = random.gauss(mu, sig)
            f.write(f"{rd}\n")
    typer.echo(f"data file: {file_path}, mean: {mu}, std dev: {sig}")


if __name__ == "__main__":
    app()

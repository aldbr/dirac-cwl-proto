import typer
from rich.console import Console

app = typer.Typer()
console = Console()

output: str = typer.Option("fit.txt", "--output", "-o", help="Output file")


def gaussian_fit(input_data: str):
    """Dummy gaussian fit."""
    with open(input_data, "r") as f:
        data = f.readlines()
        mu = sum(map(float, data)) / len(data)
        sigma = sum([(float(x) - mu) ** 2 for x in data]) / len(data)
        console.print(f"Mean: {mu}, Std dev: {sigma}")
        return mu, sigma


@app.command()
def main(input_data_files: list[str], output_data=output):
    for input_data in input_data_files:
        mu, sigma = gaussian_fit(input_data)
        with open(output_data, "a") as f:
            f.write(f"{input_data}: Mean: {mu}, Std dev: {sigma}\n")


if __name__ == "__main__":
    app()

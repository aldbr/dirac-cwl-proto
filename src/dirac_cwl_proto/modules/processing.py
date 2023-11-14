import typer
from rich.console import Console
from pathlib import Path
import random

app = typer.Typer()
console = Console()

@app.command()
def process(files: str = typer.Argument(..., help="Path to the input files")):
    # Processing the input files
    input_files = files.split(',')
    for file in input_files:
        console.print(f"Processing file: [bold green]{file}[/bold green]")
        # Simulate processing by creating an output file
        output_file = Path(file).stem + "_processed.txt"
        with open(output_file, 'w') as f:
            f.write(f"Processed content of {file}\n")

    console.print("[bold yellow]Processing completed[/bold yellow]")

if __name__ == "__main__":
    app()


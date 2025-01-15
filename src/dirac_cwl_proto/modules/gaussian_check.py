import time

import typer
from rich.console import Console

app = typer.Typer()
console = Console()

@app.command()
def benchmark(input_data: str, output_data: str):
    start_time = time.time()
    
    with open(input_data, 'r') as f:
        data = f.readlines()
        # check that the data are represented by a gaussian distribution
        mu = sum(map(float, data)) / len(data)
        sig = sum([(float(x) - mu) ** 2 for x in data]) / len(data)
        console.print(f"Mean: {mu}, Standard deviation: {sig}")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    console.print(f"Elapsed time: {elapsed_time} seconds")
    with open(output_data, 'w') as f:
        f.write(f"Mean: {mu}, Standard deviation: {sig}, Time: {elapsed_time}\n")

if __name__ == "__main__":
    app()

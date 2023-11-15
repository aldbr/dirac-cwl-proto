import random
import json
import typer
from rich.console import Console

app = typer.Typer()
console = Console()

@app.command()
def simulate(simulation_config: str = typer.Argument(..., help="Configuration file")):
    with open(simulation_config) as file:
        config = json.load(file)

    # Generate a random result for simulation
    result_sim = random.random()
    input_data_query_param = random.randint(config["min-random"], config["max-random"])

    # Writing outputs to files
    with open('result_sim.txt', 'w') as file:
        file.write(str(result_sim))

    with open('input_data_query_parameters.json', 'w') as file:
        json.dump({'input_data_query_param': input_data_query_param}, file)

    # Displaying results on the console
    console.print(f"Simulation Result: [bold green]{result_sim}[/bold green]")
    console.print(f"Input Data Query Parameter: [bold yellow]{input_data_query_param}[/bold yellow]")

if __name__ == "__main__":
    app()


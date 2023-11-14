import json
import typer
from rich.console import Console

app = typer.Typer()
console = Console()

@app.command()
def resolve(input_file: str = typer.Argument(..., help="Path to the input file containing the data query parameter")):
    # Reading the input data query parameter
    with open(input_file, 'r') as file:
        data = json.load(file)
        input_data_query_param = data['input_data_query_param']

    # Process the input data query parameter
    input_data_query = f"Processed: {input_data_query_param}"

    # Writing the output to a file
    with open('input_data_query.txt', 'w') as file:
        file.write(input_data_query)

    # Displaying the result on the console
    console.print(f"Input Data Query: [bold blue]{input_data_query}[/bold blue]")

if __name__ == "__main__":
    app()


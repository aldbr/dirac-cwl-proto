import yaml
import typer
from pathlib import Path
from rich.markup import escape
from utils import validate_cwl, console

app = typer.Typer()

def create_cwl_file(step_name: str, step_content: str, output_dir: str):
    """Creates a CWL file for a given step."""
    file_name = f"{step_name}.cwl"
    with open(output_dir / file_name, 'w') as file:
        yaml.dump(step_content, file)
    console.print(f"[green]Created[/green] [bold]{file_name}[/bold]")

@app.command()
def split_cwl_workflow(cwl_file: str):
    """Splits a CWL workflow into individual step files."""
    if not validate_cwl(cwl_file):
        console.print(":x: [red]Invalid CWL file. Cannot proceed.[/red]")
        raise typer.Exit()

    with open(cwl_file, 'r') as file:
        workflow = yaml.safe_load(file)

    if 'steps' not in workflow:
        console.print(":x: [red]No steps found in the workflow.[/red]", style="bold")
        return

    output_dir = Path(cwl_file).parent / "split_steps"
    output_dir.mkdir(exist_ok=True)

    for step_name, step_content in workflow['steps'].items():
        create_cwl_file(step_name, step_content['run'], output_dir)

    check_mark = escape("✔")  # Unicode for a check mark
    console.print(f"[green]{check_mark}[/green] Workflow split into individual steps.")

if __name__ == "__main__":
    app()


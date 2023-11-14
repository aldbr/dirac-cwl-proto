import subprocess
import typer
from rich.console import Console
from rich.markup import escape
from utils import validate_cwl, console

app = typer.Typer()

@app.command()
def run_workflow(cwl_file: str):
    """
    Executes a given CWL workflow using cwltool.
    """
    if not validate_cwl(cwl_file):
        console.print(":x: [red]Invalid CWL file. Cannot proceed.[/red]")
        raise typer.Exit()

    try:
        console.print(f"Executing workflow: [yellow]{cwl_file}[/yellow]")
        result = subprocess.run(["cwltool", cwl_file], capture_output=True, text=True)

        if result.returncode == 0:
            check_mark = escape("✔")  # Unicode for a check mark
            console.print(f"[green]{check_mark} Workflow executed successfully.[/green]")
        else:
            console.print(f":x: [red]Error in executing workflow:[/red] \n{result.stderr}")

    except Exception as e:
        console.print(f":x: [red]Failed to execute workflow: {e}[/red]")

if __name__ == "__main__":
    app()


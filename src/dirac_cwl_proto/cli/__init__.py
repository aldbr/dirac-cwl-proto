import typer
from . import production, job

app = typer.Typer()

app.add_typer(production.app, name="production")
app.add_typer(job.app, name="job")

@app.command()
def run_workflow(cwl_path: str):
    """
    Run a complex CWL workflow from end to end.
    """
    # Split the CWL workflow into individual steps
    split_cwl_paths = production.split_cwl_workflow(cwl_path)

    # Execute each split CWL workflow
    for split_cwl_path in split_cwl_paths:
        job.run_workflow(split_cwl_path)

if __name__ == "__main__":
    app()

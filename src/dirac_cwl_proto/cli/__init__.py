import logging
import subprocess
import typer
import yaml

from pathlib import Path
from pydantic import BaseModel, validator
from typing import List

from .utils import validate_cwl, console

app = typer.Typer()


@app.command()
def run(workflow_path: str = typer.Argument(..., help="Path to the CWL file"),):
    """
    Run a workflow from end to end (production/transformation/job).
    """
    # Generate a production
    console.print(f"[blue]:information_source:[/blue] [bold]Creating new production based on {workflow_path}...[/bold]")
    try:
        production = create_production(workflow_path)
    except (ValueError, RuntimeError) as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] {ex}")
        return typer.Exit(code=1)
    console.print(f"[green]:heavy_check_mark:[/green] [bold]Production created: ready to start.[/bold]")

    # Start the production
    console.print(f"[blue]:information_source:[/blue] [bold]Starting the production...[/bold]")
    start_production(production) 
    console.print(f"[green]:heavy_check_mark:[/green] [bold]Production started.[/bold]")

# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------

class JobModel(BaseModel):
    workflow_path: str

    @validator('workflow_path')
    def validate_cwl_workflow(cls, workflow_path):
        if not validate_cwl(workflow_path):
            raise ValueError(f"Invalid CWL file: {workflow_path}")
        return workflow_path


class TransformationModel(BaseModel):
    workflow_path: str
    jobs: List[JobModel] | None = None

    @validator('workflow_path')
    def validate_cwl_workflow(cls, workflow_path):
        if not validate_cwl(workflow_path):
            raise ValueError(f"Invalid CWL file: {workflow_path}")
        return workflow_path


class ProductionModel(BaseModel):
    workflow_path: str
    transformations: List[TransformationModel] | None = None

    @validator('workflow_path')
    def validate_cwl_workflow(cls, workflow_path):
        if not validate_cwl(workflow_path):
            raise ValueError(f"Invalid CWL file: {workflow_path}")
        return workflow_path

# -----------------------------------------------------------------------------
# Production management
# -----------------------------------------------------------------------------

def create_production(workflow_path: str) -> ProductionModel:
    """Validate a CWL workflow and create transformations from them."""

    logging.info(f"Creating production from workflow: {workflow_path}...")
    # Validate the main workflow
    production = ProductionModel(workflow_path=workflow_path)

    # Get the workflow content
    with open(workflow_path, 'r') as file:
        workflow = yaml.safe_load(file)

    if 'steps' not in workflow:
        raise RuntimeError("No steps found in the workflow.")

    logging.info(f"Production validated and created!")
    logging.info(f"Creating transformations from workflow: {workflow_path}...")

    # Create a directory to store the subworkflows
    output_dir = Path(workflow_path).parent / "subworkflows"
    output_dir.mkdir(exist_ok=True)

    # Create a subworkflow and a transformation for each step
    transformations = []
    for step_name, step_content in workflow['steps'].items():
        subworkflow = _create_subworkflow(step_name, step_content['run'], output_dir)
        transformations.append(TransformationModel(workflow_path=subworkflow.as_posix()))

    production.transformations = transformations
    return production

def _create_subworkflow(step_name: str, step_content: str, output_dir: str) -> Path:
    """Create a CWL file for a given step."""
    file_name = output_dir / f"{step_name}.cwl"
    
    # Add the cwlVersion to the step
    step_content["cwlVersion"] = "v1.2"
    
    # Write the step to a file
    with open(file_name, 'w') as file:
        yaml.dump(step_content, file)
    return file_name

def start_production(production: ProductionModel):
    """Start a production."""
    logging.info(f"Starting transformations")
    for transformation in production.transformations:
        start_transformation(transformation)

# -----------------------------------------------------------------------------
# Transformation management
# -----------------------------------------------------------------------------

def create_transformation(workflow_path: str):
    """Validate a CWL workflow."""
    transformation = TransformationModel(workflow_path=workflow_path)
    return transformation


def start_transformation(transformation: TransformationModel):
    """Start a transformation."""
    # Should probably be done as a thread
    logging.info(f"Submit jobs")
    submit_job(transformation.workflow_path)

# -----------------------------------------------------------------------------
# Job management
# -----------------------------------------------------------------------------

def submit_job(workflow_path: str):
    """
    Executes a given CWL workflow using cwltool.
    """
    job = JobModel(workflow_path=workflow_path)

    try:
        console.print(f"Executing workflow: [yellow]{workflow_path}[/yellow]")
        result = subprocess.run(["cwltool", workflow_path], capture_output=True, text=True)

        if result.returncode == 0:
            console.print(f"[green]:heavy_check_mark: Workflow executed successfully.[/green]")
        else:
            console.print(f":x: [red]Error in executing workflow:[/red] \n{result.stderr}")

    except Exception as e:
        console.print(f":x: [red]Failed to execute workflow: {e}[/red]")

if __name__ == "__main__":
    app()

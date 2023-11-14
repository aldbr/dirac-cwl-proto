import subprocess
import typer
import yaml

from pathlib import Path
from pydantic import BaseModel, validator
from typing import List

from .utils import validate_cwl, console

app = typer.Typer()


@app.command()
def run(cwl_file: str , sub_cwl_files: List[str] | None = None):
    """
    Run a workflow from end to end (production/transformation/job).
    """
    # Generate a production
    try:
        production = create_production(cwl_file=cwl_file, sub_cwl_files=sub_cwl_files)
    except ValueError as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] {ex}")
        return typer.Exit(code=1)

    
    console.print(f"[green]:heavy_check_mark:[/green] Production created: ready to start.")

    # 
    # production.start()
    # for transformation_id in production.transformation_ids:
    #     trans.start()
    # 
    # based on input plugins submit jobs    


    # Execute each split CWL workflow
    #transformation_ids = []
    #for sub_cwl_file in sub_cwl_files:
    #    transformation_ids.append(transformation.create(sub_cwl_file))

    #for transformation_id in transformation_ids:
    #    job.create(transformation_id)

# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------

class JobModel(BaseModel):
    cwl_file: str

    @validator('cwl_file')
    def validate_main_cwl_file(cls, cwl_file):
        if not validate_cwl(cwl_file):
            raise ValueError(f"Invalid CWL file: {cwl_file}")
        return cwl_file


class TransformationModel(BaseModel):
    cwl_file: str
    jobs: List[JobModel] | None

    @validator('cwl_file')
    def validate_main_cwl_file(cls, cwl_file):
        if not validate_cwl(cwl_file):
            raise ValueError(f"Invalid CWL file: {cwl_file}")
        return cwl_file


class ProductionModel(BaseModel):
    cwl_file: str
    sub_cwl_files: List[str] | None
    transformations: List[TransformationModel] | None

    @validator('cwl_file')
    def validate_main_cwl_file(cls, cwl_file):
        if not validate_cwl(cwl_file):
            raise ValueError(f"Invalid CWL file: {cwl_file}")
        return cwl_file

    @validator('sub_cwl_files')
    def validate_sub_cwl_files(cls, sub_cwl_files):
        if not sub_cwl_files:
            return sub_cwl_files

        for file in sub_cwl_files:
            if not validate_cwl(file):
                raise ValueError(f"Invalid CWL file: {file}")
        return sub_cwl_files

# -----------------------------------------------------------------------------
# Production management
# -----------------------------------------------------------------------------

def create_production(cwl_file: str , sub_cwl_files: List[str] | None = None):
    """Validate a CWL workflow and subworkflows and create transformations from them."""

    production = ProductionModel(cwl_file=cwl_file, sub_cwl_files=sub_cwl_files)

    with open(cwl_file, 'r') as file:
        workflow = yaml.safe_load(file)

    if 'steps' not in workflow:
        console.print("[red]:heavy_multiplication_x:[/red]No steps found in the workflow.", style="bold")
        return

    output_dir = Path(cwl_file).parent / "split_steps"
    output_dir.mkdir(exist_ok=True)

    for step_name, step_content in workflow['steps'].items():
        create_cwl_file(step_name, step_content['run'], output_dir)
    
    return production


def create_cwl_file(step_name: str, step_content: str, output_dir: str):
    """Creates a CWL file for a given step."""
    file_name = f"{step_name}.cwl"
    with open(output_dir / file_name, 'w') as file:
        yaml.dump(step_content, file)
    console.print(f"[green]Created[/green] [bold]{file_name}[/bold]")

# -----------------------------------------------------------------------------
# Transformation management
# -----------------------------------------------------------------------------



# -----------------------------------------------------------------------------
# Job management
# -----------------------------------------------------------------------------



def submit_job(cwl_file: str):
    """
    Executes a given CWL workflow using cwltool.
    """
    job = JobModel(cwl_file)

    try:
        console.print(f"Executing workflow: [yellow]{cwl_file}[/yellow]")
        result = subprocess.run(["cwltool", cwl_file], capture_output=True, text=True)

        if result.returncode == 0:
            console.print(f"[green]:heavy_check_mark: Workflow executed successfully.[/green]")
        else:
            console.print(f":x: [red]Error in executing workflow:[/red] \n{result.stderr}")

    except Exception as e:
        console.print(f":x: [red]Failed to execute workflow: {e}[/red]")

if __name__ == "__main__":
    app()

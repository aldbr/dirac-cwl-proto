import glob
import logging
import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

import typer
import yaml
from cwl_utils.parser import load_document_by_uri, save
from cwl_utils.parser.cwl_v1_2 import CommandLineTool, File, Workflow, WorkflowStep
from pydantic import BaseModel, PrivateAttr, ValidationError, model_validator, validator
from rich.console import Console
from rich.text import Text

from .metadata_models import BasicMetadataModel, IMetadataModel, LHCbMetadataModel

app = typer.Typer()
console = Console()


@app.command()
def run(
    workflow_path: str = typer.Argument(..., help="Path to the CWL file"),
    metadata_path: str = typer.Argument(
        ..., help="Path to the file containing the metadata"
    ),
    metadata_type: str = typer.Argument(
        ..., help="Type of metadata to use", case_sensitive=False
    ),
):
    """
    Run a workflow from end to end (production/transformation/job).

    This command will:
    - Validate the workflow
    - Create a production from the workflow
    - Start the production
    - Start the transformations
    - Start the jobs
    """
    # Add the modules directory to the PATH
    modules_path = Path(__file__).resolve().parent.parent / "modules"
    os.environ["PATH"] += os.pathsep + str(modules_path)

    # Generate a production
    console.print(
        f"[blue]:information_source:[/blue] [bold]Creating new production based on {workflow_path}...[/bold]"
    )
    try:
        production = create_production(workflow_path, metadata_path, metadata_type)
    except (ValueError, RuntimeError) as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] {ex}")
        return typer.Exit(code=1)
    console.print(
        "[green]:heavy_check_mark:[/green] [bold]Production created: ready to start.[/bold]"
    )

    # Start the production
    console.print(
        "[blue]:information_source:[/blue] [bold]Starting the production...[/bold]"
    )
    if not start_production(production):
        console.print(
            "[red]:heavy_multiplication_x:[/red] [bold]Failed to start production.[/bold]"
        )
        return typer.Exit(code=1)
    console.print("[green]:heavy_check_mark:[/green] [bold]Production started.[/bold]")


# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------


class CWLBaseModel(BaseModel):
    """Base class for CWL models."""

    workflow_path: str
    workflow: CommandLineTool | Workflow | None = None

    metadata_path: str
    metadata_type: str
    metadata: IMetadataModel | None = None

    id: int | None = None
    _id_counter = 0  # Class variable to keep track of the last assigned ID
    _id_lock = PrivateAttr(
        default_factory=Lock
    )  # Lock to ensure thread-safe access to _id_counter

    def __init__(self, **data):
        super().__init__(**data)
        with self._id_lock:  # Acquire lock for thread-safe ID increment
            self.id = self._id_counter
            self._id_counter += 1

    @validator("workflow_path")
    def validate_workflow(cls, workflow_path):
        if not validate_cwl(workflow_path):
            raise ValueError(f"Invalid CWL file: {workflow_path}")
        return workflow_path

    @validator("metadata_path")
    def validate_metadata(cls, metadata_path):
        if metadata_path and not os.path.isfile(metadata_path):
            raise ValueError(f"Metadata file does not exist: {metadata_path}")
        return metadata_path

    @model_validator(mode="after")
    def load_workflow_metadata(self):
        """Load the workflow and metadata files."""
        metadata_models = {
            "basic": BasicMetadataModel,
            "lhcb": LHCbMetadataModel,
        }
        try:
            self.workflow = load_document_by_uri(self.workflow_path)

            with open(self.metadata_path, "r") as file:
                metadata = yaml.safe_load(file)
            # Dynamically create a metadata model based on the metadata type
            self.metadata = metadata_models[self.metadata_type](  # noqa
                **{dash_to_snake_case(k): v for k, v in metadata.items()}
            )
        except Exception as e:
            raise ValidationError(f"Failed to load workflow and metadata: {e}") from e

    class Config:
        # Allow arbitrary types to be passed to the model
        arbitrary_types_allowed = True


class JobModel(CWLBaseModel):
    """A job is a single execution of a transformation."""

    pass


class TransformationModel(CWLBaseModel):
    """A transformation is a step in a production."""

    jobs: list[JobModel] | None = None
    # Needs to be set to True if the transformation is waiting
    # for an input from another transformation
    external_inputs: dict[str, str]


class ProductionModel(CWLBaseModel):
    """A production is a set of transformations that are executed in parallel."""

    transformations: list[TransformationModel] | None = None


# -----------------------------------------------------------------------------
# Production management
# -----------------------------------------------------------------------------


def create_production(
    workflow_path: str, metadata_path: str, metadata_type: str
) -> ProductionModel:
    """Validate a CWL workflow and create transformations from them.

    :param workflow_path: Path to the CWL workflow
    :param metadata_path: Path to the metadata file

    :return: The production
    """

    logging.info(f"Creating production from workflow: {workflow_path}...")
    # Validate the main workflow
    production = ProductionModel(
        workflow_path=workflow_path,
        metadata_path=metadata_path,
        metadata_type=metadata_type,
    )

    if not production.workflow or production.workflow.steps:
        raise RuntimeError("No steps found in the workflow.")

    logging.info("Production validated and created!")
    logging.info(f"Creating transformations from workflow: {workflow_path}...")

    # Create a subworkflow and a transformation for each step
    transformations = []
    for step in production.workflow.steps:
        step_name = step.id.split("#")[-1]
        subworkflow = _create_subworkflow(step, production.workflow.cwlVersion)
        external_inputs = _get_external_inputs(step, production.workflow.steps)

        output_dir = Path(workflow_path).parent / "subworkflows" / step_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Write the step in the step directory
        subworkflow_dict = save(subworkflow)

        output_path = output_dir / f"{step_name}.cwl"
        with open(output_path, "w") as file:
            yaml.dump(subworkflow_dict, file)

        # Copy the metadata in the step directory
        transformation_metadata_path = output_dir / "inputs.yml"
        shutil.copyfile(metadata_path, transformation_metadata_path)

        transformations.append(
            TransformationModel(
                workflow_path=str(output_path),
                metadata_path=str(transformation_metadata_path),
                metadata_type=metadata_type,
                external_inputs=external_inputs,
            )
        )
    production.transformations = transformations
    return production


def _create_subworkflow(
    step: WorkflowStep, cwlVersion: str
) -> Workflow | CommandLineTool:
    """Create a CWL file for a given step.

    If the step is a workflow, a new workflow is created.
    If the step is a command line tool, a new command line tool is created.

    :param step: The step to create a CWL file for
    :param cwlVersion: The CWL version to use

    :return: The CWL subworkflow
    """
    if step.run.class_ == "Workflow":
        # Handle nested workflows
        new_workflow = Workflow(
            cwlVersion=cwlVersion,
            inputs=step.run.inputs,
            outputs=step.run.outputs,
            steps=step.run.steps,
            requirements=step.run.requirements,
        )
    else:
        # Handle command line tools
        new_workflow = CommandLineTool(
            cwlVersion=cwlVersion,
            baseCommand=step.run.baseCommand,
            inputs=step.run.inputs,
            outputs=step.run.outputs,
            requirements=step.run.requirements,
        )
    return new_workflow


def _get_external_inputs(step: WorkflowStep, steps: list[WorkflowStep]) -> dict:
    """Get the external inputs of a step.

    :param step: The step to get the external inputs for
    :param steps: The list of all steps in the workflow

    :return: A dictionary of external inputs
    """
    external_inputs = {}
    for input in step.in_:
        # Check if the input is connected to an output of another step
        # input_.source is in the format:
        # - file://workflow.cwl#step_id/output_id: if the input is from another step
        # - file://workflow.cwl#output_id: if the input is from the main workflow
        external_input = input.source.split("#")[-1].split("/")
        if len(external_input) == 1:
            continue

        source_step_name, source_output_id = external_input
        source_step = next(
            (s for s in steps if s.id.split("#")[-1] == source_step_name), None
        )
        if not source_step:
            # Theoretically, this should never happen as the workflow is validated
            continue

        output_binding = next(
            (
                o.outputBinding.glob
                for o in source_step.run.outputs
                if o.id.split("/")[-1] == source_output_id
            ),
            None,
        )
        input_id = input.id.split("#")[-1].split("/")[-1]
        external_inputs[input_id] = output_binding
    return external_inputs


def start_production(production: ProductionModel) -> bool:
    """Start a production.

    A production is a set of transformations that are executed in parallel.

    :param production: The production to start

    :return: True if the production executed successfully, False otherwise
    """
    logging.info("Starting transformations")

    with ThreadPoolExecutor() as executor:
        transformations = production.transformations or []  # Handle None case
        results = list(executor.map(start_transformation, transformations))
    return all(results)


# -----------------------------------------------------------------------------
# Transformation management
# -----------------------------------------------------------------------------


def start_transformation(transformation: TransformationModel) -> bool:
    """Start a transformation.

    If the transformation is waiting for an input from another transformation,
    it will wait for the input to be available in the "bookkeeping".

    :param transformation: The transformation to start

    :return: True if the transformation executed successfully, False otherwise
    """
    if not transformation.metadata:
        raise RuntimeError("Transformation metadata is not set.")

    # If there is an input data, wait for it to be available in the "bookkeeping"
    if transformation.external_inputs:
        while not (inputs := _get_inputs(transformation)):
            time.sleep(5)

        # Update the input data in the metadata
        cwl_inputs = {}
        for input_id, input_paths in inputs.items():
            cwl_input_paths = []
            for input_path in input_paths:
                cwl_input_paths.append(File(path=input_path))
            cwl_inputs[input_id] = save(cwl_input_paths, relative_uris=False)

        # Update the metadata with the input data
        metadata = transformation.metadata.model_dump()
        metadata.update({dash_to_snake_case(k): v for k, v in cwl_inputs.items()})
        with open(transformation.metadata_path, "w") as file:
            yaml.dump({snake_case_to_dash(k): v for k, v in metadata.items()}, file)

        transformation = TransformationModel(
            workflow_path=transformation.workflow_path,
            metadata_path=transformation.metadata_path,
            metadata_type=transformation.metadata_type,
            external_inputs=transformation.external_inputs,
        )

    logging.info("Submitting jobs")
    return submit_job(
        workflow_path=transformation.workflow_path,
        metadata_path=transformation.metadata_path,
        metadata_type=transformation.metadata_type,
    )


def _get_inputs(transformation: TransformationModel) -> dict[str, list[str]] | None:
    """Get the input data from the "bookkeeping".

    :param transformation: The transformation that needs the input

    :return: The paths to the input data
    """
    if not transformation.metadata:
        raise RuntimeError("Transformation metadata is not set.")

    # Check if the input is available in the "bookkeeping"
    inputs = {}
    for metadata_name, expected_input in transformation.external_inputs.items():
        input_paths = glob.glob(
            str(transformation.metadata.get_bk_path() / expected_input)
        )
        if not input_paths:
            continue

        inputs[metadata_name] = [str(Path(path).resolve()) for path in input_paths]

    if not _is_ready(inputs):
        return None

    return inputs


def _is_ready(inputs: dict[str, list[str]]) -> bool:
    """Check if the input is ready.

    :param inputs: The input to check

    :return: True if the input is ready, False otherwise
    """
    return all(inputs.values())


# -----------------------------------------------------------------------------
# Job management
# -----------------------------------------------------------------------------


def submit_job(workflow_path: str, metadata_path: str, metadata_type: str) -> bool:
    """
    Executes a given CWL workflow using cwltool.
    This is the equivalent of the DIRAC JobWrapper.

    :param workflow_path: Path to the CWL workflow
    :param metadata_path: Path to the metadata file
    :param metadata_type: Type of metadata to use

    :return: True if the workflow executed successfully, False otherwise
    """
    job = JobModel(
        workflow_path=workflow_path,
        metadata_path=metadata_path,
        metadata_type=metadata_type,
    )

    try:
        console.print(f"Executing workflow: [yellow]{workflow_path}[/yellow]")
        result = subprocess.run(
            ["cwltool", workflow_path, metadata_path], capture_output=True, text=True
        )

        if result.returncode != 0:
            console.print(
                f":x: [red]Error in executing workflow:[/red] \n{Text.from_ansi(result.stderr)}"
            )
            return False

        # Post process the output: store the output in the "bookkeeping"
        outputs = glob.glob("*.sim")
        if outputs and job.metadata:
            _store_output(job, outputs[0])

        console.print(
            "[green]:heavy_check_mark: Workflow executed successfully.[/green]"
        )
        return True

    except Exception as e:
        console.print(f":x: [red]Failed to execute workflow: {e}[/red]")
        return False


def _store_output(job: JobModel, output: str):
    """Store the output in the "bookkeeping" directory."""
    if not job.metadata:
        raise RuntimeError("Job metadata is not set.")
    # Create the "bookkeeping" path
    output_path = job.metadata.get_bk_path()

    # Send the output to the "bookkeeping"
    bk_output = output_path / f"{job.id}_{output}"
    os.rename(output, bk_output)
    logging.info(f"Output stored in {bk_output}")


# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------


def validate_cwl(cwl_file: str):
    """Validates a CWL file."""
    result = subprocess.run(
        ["cwltool", "--validate", cwl_file], capture_output=True, text=True
    )
    if result.returncode == 0:
        return True

    logging.error(Text.from_ansi(result.stderr))
    return False


def dash_to_snake_case(name):
    """Converts a string from dash-case to snake_case."""
    return name.replace("-", "_")


def snake_case_to_dash(name):
    """Converts a string from snake_case to dash-case."""
    return name.replace("_", "-")


if __name__ == "__main__":
    app()

"""
CLI interface to run a workflow as a production.
"""
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import typer
from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import CommandLineTool, Workflow, WorkflowStep
from rich.console import Console
from ruamel.yaml import YAML

from dirac_cwl_proto.transformation import (
    ExternalInputModel,
    TransformationModel,
    start_transformation,
)
from dirac_cwl_proto.utils import CWLBaseModel

app = typer.Typer()
console = Console()


@app.command("submit")
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
            "[red]:heavy_multiplication_x:[/red] [bold]Failed to run production.[/bold]"
        )
        return typer.Exit(code=1)
    console.print("[green]:heavy_check_mark:[/green] [bold]Production done.[/bold]")


# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------


# TODO: workflow should be a Workflow, not a CommandLineTool
# TODO: inputs should not be composed of relative path(?)
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

    workflow = cast(Workflow, production.workflow)
    if not (workflow and workflow.steps):
        raise RuntimeError("No steps found in the workflow.")

    logging.info("Production validated and created!")
    logging.info(f"Creating transformations from workflow: {workflow_path}...")

    # Create a subworkflow and a transformation for each step
    transformations = []
    for step in workflow.steps:
        step_name = step.id.split("#")[-1]
        subworkflow = _create_subworkflow(step, str(workflow.cwlVersion))
        external_inputs = _get_external_inputs(step, workflow.steps)

        output_dir = Path(workflow_path).parent / "subworkflows" / step_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Write the step in the step directory
        subworkflow_dict = save(subworkflow)

        output_path = output_dir / f"{step_name}.cwl"
        with open(output_path, "w") as file:
            YAML().dump(subworkflow_dict, file)

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
    new_workflow: Workflow | CommandLineTool
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
            arguments=step.run.arguments,
            baseCommand=step.run.baseCommand,
            inputs=step.run.inputs,
            outputs=step.run.outputs,
            requirements=step.run.requirements,
        )
    return new_workflow


def _get_external_inputs(
    step: WorkflowStep, steps: list[WorkflowStep]
) -> dict[str, ExternalInputModel]:
    """Get the external inputs of a step.

    :param step: The step to get the external inputs for
    :param steps: The list of all steps in the workflow

    :return: A dictionary of external inputs
    """
    external_inputs = {}
    for input in step.in_:
        # Check if the input is connected to an output of another step
        # input.source is in the format:
        # - file://workflow.cwl#step_id/output_id: if the input is from another step
        # - file://workflow.cwl#output_id: if the input is from the main workflow
        external_input = input.source.split("#")[-1].split("/")
        if len(external_input) == 1:
            continue

        source_step_name, source_output_id = external_input
        source_step = next(
            (s for s in steps if s.id and s.id.split("#")[-1] == source_step_name), None
        )
        if not source_step:
            # Theoretically, this should never happen as the workflow is validated
            continue

        output = next(
            (
                o
                for o in source_step.run.outputs
                if o.id.split("/")[-1] == source_output_id
            ),
            None,
        )
        if not output:
            # Theoretically, this should never happen as the workflow is validated
            continue

        if source_step.run.class_ == "CommandLineTool":
            input_id = input.id.split("#")[-1].split("/")[-1]
            external_inputs[input_id] = ExternalInputModel(
                path=output.outputBinding.glob, class_=output.type_
            )
        else:
            _, subsource_output_id = output.outputSource.split("#")
            subsource_step_id = os.path.dirname(subsource_output_id)

            # If the source step is a workflow, the output is available in one of the substeps of source_step
            subsource_step = next(
                (
                    s
                    for s in source_step.run.steps
                    if s.id.split("#")[-1] == subsource_step_id
                ),
                None,
            )
            if not subsource_step:
                # Theoretically, this should never happen as the workflow is validated
                continue

            subsource_step_output = next(
                (
                    o
                    for o in subsource_step.run.outputs
                    if o.id.split("/")[-1] == os.path.basename(subsource_output_id)
                ),
                None,
            )
            if not subsource_step_output:
                # Theoretically, this should never happen as the workflow is validated
                continue

            input_id = input.id.split("#")[-1].split("/")[-1]
            external_inputs[input_id] = ExternalInputModel(
                path=subsource_step_output.outputBinding.glob,
                class_=subsource_step_output.type_,
            )
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

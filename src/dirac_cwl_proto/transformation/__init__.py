"""
CLI interface to run a workflow as a transformation.
"""
import glob
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import typer
from cwl_utils.pack import pack
from cwl_utils.parser import load_document
from cwl_utils.parser.cwl_v1_2 import File
from rich import print_json
from rich.console import Console
from ruamel.yaml import YAML
from schema_salad.exceptions import ValidationException

from dirac_cwl_proto.job import submit_job_router
from dirac_cwl_proto.submission_models import (
    JobDescriptionModel,
    JobParameterModel,
    JobSubmissionModel,
    TransformationMetadataModel,
    TransformationSubmissionModel,
)
from dirac_cwl_proto.utils import _get_metadata

app = typer.Typer()
console = Console()


# -----------------------------------------------------------------------------
# dirac-cli commands
# -----------------------------------------------------------------------------


@app.command("submit")
def submit_transformation_client(
    task_path: str = typer.Argument(..., help="Path to the CWL file"),
    metadata_path: Optional[str] = typer.Option(
        None, help="Path to metadata file used to generate the input query"
    ),
    # Dirac-specific parameters that are used by the jobs
    platform: Optional[str] = typer.Option(
        None, help="The platform required to run the transformation"
    ),
    priority: Optional[int] = typer.Option(
        10, help="The priority of the transformation"
    ),
    sites: Optional[List[str]] = typer.Option(
        None, help="The site to run the transformation"
    ),
    # Specific parameter for the purpose of the prototype
    local: Optional[bool] = typer.Option(
        True, help="Run the jobs locally instead of submitting them to the router"
    ),
):
    """
    Correspond to the dirac-cli command to submit transformations

    This command will:
    - Validate the workflow
    - Start the transformation
    """
    # Validate the workflow
    console.print(
        "[blue]:information_source:[/blue] [bold]CLI:[/bold] Validating the transformation..."
    )
    try:
        task = load_document(pack(task_path))
    except FileNotFoundError as ex:
        console.print(
            f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to load the task:\n{ex}"
        )
        return typer.Exit(code=1)
    except ValidationException as ex:
        console.print(
            f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to validate the task:\n{ex}"
        )
        return typer.Exit(code=1)
    console.print(f"\t[green]:heavy_check_mark:[/green] Task {task_path}")

    # Load the metadata: at this stage, only the structure is validated, not the content
    metadata_model = None
    if metadata_path:
        with open(metadata_path, "r") as file:
            metadata = YAML(typ="safe").load(file)
        metadata_model = TransformationMetadataModel(**metadata)
    else:
        metadata_model = TransformationMetadataModel()
    console.print("\t[green]:heavy_check_mark:[/green] Metadata")

    transformation_description = JobDescriptionModel(
        platform=platform,
        priority=priority,
        sites=sites,
    )
    console.print("\t[green]:heavy_check_mark:[/green] Description")

    transformation = TransformationSubmissionModel(
        task=task,
        metadata=metadata_model,
        description=transformation_description,
    )
    console.print(
        "[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Transformation validated."
    )

    # Submit the transformation
    console.print(
        "[blue]:information_source:[/blue] [bold]CLI:[/bold] Submitting the transformation..."
    )
    print_json(transformation.model_dump_json(indent=4))
    if not submit_transformation_router(transformation):
        console.print(
            "[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to run transformation."
        )
        return typer.Exit(code=1)
    console.print(
        "[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Transformation done."
    )


# -----------------------------------------------------------------------------
# dirac-router commands
# -----------------------------------------------------------------------------


def submit_transformation_router(transformation: TransformationSubmissionModel) -> bool:
    """
    Execute a transformation using the router.
    If the transformation is waiting for an input from another transformation,
    it will wait for the input to be available in the "bookkeeping".

    :param transformation: The transformation to start

    :return: True if the transformation executed successfully, False otherwise
    """
    logger = logging.getLogger("TransformationRouter")

    # Validate the transformation
    logger.info("Validating the transformation...")
    # Already validated by the pydantic model
    logger.info("Transformation validated!")

    # Check if the transformation is waiting for an input
    # - if there is no metadata, the transformation is not waiting for an input and can go on
    # - if there is metadata, the transformation is waiting for an input
    job_model_params = []
    if transformation.metadata.query_params and transformation.metadata.group_size:
        # Get the metadata class
        transformation_metadata = _get_metadata(transformation)

        # Build the input cwl for the jobs to submit
        logger.info("Getting the input data for the transformation...")
        input_data_dict = {}
        min_length = None
        for input_name, group_size in transformation.metadata.group_size.items():
            # Get input query
            logger.info(f"\t- Getting input query for {input_name}...")
            input_query = transformation_metadata.get_input_query(input_name)
            if not input_query:
                raise RuntimeError("Input query not found.")

            # Wait for the input to be available
            logger.info(f"\t- Waiting for input data for {input_name}...")
            logger.debug(f"\t\t- Query: {input_query}")
            logger.debug(f"\t\t- Group Size: {group_size}")
            while not (inputs := _get_inputs(input_query, group_size)):
                logger.debug(f"\t\t- Result: {inputs}")
                time.sleep(5)
            logger.info(f"\t- Input data for {input_name} available.")
            if not min_length or len(inputs) < min_length:
                min_length = len(inputs)

            # Update the input data in the metadata
            # Only keep the first min_length inputs
            input_data_dict[input_name] = inputs[:min_length]

        # Get the JobModelParameter for each input
        job_model_params = _generate_job_model_parameter(input_data_dict)
        logger.info("Input data for the transformation retrieved!")

    logger.info("Building the jobs...")
    jobs = JobSubmissionModel(
        task=transformation.task,
        parameters=job_model_params,
        description=transformation.description,
        metadata=transformation.metadata,
    )
    logger.info("Jobs built!")

    logger.info("Submitting jobs...")
    return submit_job_router(jobs)


# -----------------------------------------------------------------------------
# Transformation management
# -----------------------------------------------------------------------------


def _get_inputs(input_query: Path, group_size: int) -> List[List[str]]:
    """Get the input data from the input query.

    :param input_query: The input query to get the input data
    :param group_size: The number of jobs to group together in a transformation
    :return: A list of lists of paths to the input data, each inner list has length group_size
    """
    # TODO: how do we know whether a given input has already been processed?

    # Retrieve all input paths matching the query
    input_paths = glob.glob(str(input_query / "*"))
    len_input_paths = len(input_paths)

    # Ensure there are enough inputs to form at least one group
    if len_input_paths < group_size:
        return []

    # Calculate the number of full groups
    num_full_groups = len_input_paths // group_size

    # Group the input paths into lists of size group_size
    input_groups = [
        input_paths[i * group_size : (i + 1) * group_size]
        for i in range(num_full_groups)
    ]

    return input_groups


def _generate_job_model_parameter(
    input_data_dict: Dict[str, List[List[str]]]
) -> List[JobParameterModel]:
    """Generate job model parameters from input data provided."""
    job_model_params = []

    input_names = list(input_data_dict.keys())
    input_data_lists = [input_data_dict[input_name] for input_name in input_names]
    grouped_input_data = [
        dict(zip(input_names, elements)) for elements in zip(*input_data_lists)
    ]
    for group in grouped_input_data:
        cwl_inputs = {}
        for input_name, input_data in group.items():
            cwl_inputs[input_name] = [
                File(path=str(Path(path).resolve())) for path in input_data
            ]

        job_model_params.append(JobParameterModel(sandbox=None, cwl=cwl_inputs))

    return job_model_params

"""
CLI interface to run a workflow as a transformation.
"""
import glob
import logging
import time
from pathlib import Path
from typing import Any

import typer
from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import File
from pydantic import BaseModel
from rich.console import Console
from ruamel.yaml import YAML

from dirac_cwl_proto.job import JobModel, submit_job
from dirac_cwl_proto.utils import CWLBaseModel, dash_to_snake_case, snake_case_to_dash

app = typer.Typer()
console = Console()


# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------


class ExternalInputModel(BaseModel):
    """An external input is a file that is produced by another transformation."""

    path: str
    class_: Any


class TransformationModel(CWLBaseModel):
    """A transformation is a step in a production."""

    jobs: list[JobModel] | None = None
    external_inputs: dict[str, ExternalInputModel]


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
        for input_id, cwl_input_paths in inputs.items():
            cwl_inputs[input_id] = save(cwl_input_paths, relative_uris=False)

        # Update the metadata with the input data
        metadata = transformation.metadata.model_dump()
        metadata.update({dash_to_snake_case(k): v for k, v in cwl_inputs.items()})
        with open(transformation.metadata_path, "w") as file:
            YAML().dump({snake_case_to_dash(k): v for k, v in metadata.items()}, file)

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


def _get_inputs(transformation: TransformationModel) -> dict | None:
    """Get the input data from the "bookkeeping".

    :param transformation: The transformation that needs the input

    :return: The paths to the input data
    """
    if not transformation.metadata:
        raise RuntimeError("Transformation metadata is not set.")

    # Check if the input is available in the "bookkeeping"
    inputs: dict = {}
    for metadata_name, metadata_value in transformation.external_inputs.items():
        input_paths = glob.glob(
            str(
                transformation.metadata.get_bk_path(metadata_name) / metadata_value.path
            )
        )
        if not input_paths:
            inputs[metadata_name] = None
            continue

        # If the input should be a single file, return the path
        if (
            isinstance(metadata_value.class_, str) and metadata_value.class_ == "File"
        ) or (
            isinstance(metadata_value.class_, list) and "File" in metadata_value.class_
        ):
            inputs[metadata_name] = File(path=str(Path(input_paths[0]).resolve()))
            continue

        # If the input should be an array of files, return the paths
        inputs[metadata_name] = [
            File(str(Path(path).resolve())) for path in input_paths
        ]

    if not _is_ready(inputs):
        return None

    return inputs


def _is_ready(inputs: dict[str, list[str]]) -> bool:
    """Check if the input is ready.

    :param inputs: The input to check

    :return: True if the input is ready, False otherwise
    """
    return all(inputs.values())

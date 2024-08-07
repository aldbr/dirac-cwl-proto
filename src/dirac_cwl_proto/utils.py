"""
CLI interface to run a CWL workflow from end to end (production/transformation/job).
"""
import logging
import os
import subprocess
from threading import Lock

from cwl_utils.parser import load_document_by_uri
from cwl_utils.parser.cwl_v1_2 import CommandLineTool, Workflow
from pydantic import BaseModel, PrivateAttr, ValidationError, model_validator, validator
from rich.text import Text
from ruamel.yaml import YAML

from .metadata_models import (
    BasicMetadataModel,
    IMetadataModel,
    LHCbMetadataModel,
    MacobacMetadataModel,
    MandelBrotMetadataModel,
)

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
            "macobac": MacobacMetadataModel,
            "lhcb": LHCbMetadataModel,
            "mandelbrot": MandelBrotMetadataModel,
        }
        try:
            self.workflow = load_document_by_uri(self.workflow_path)

            with open(self.metadata_path, "r") as file:
                metadata = YAML(typ="safe").load(file)

            # Adapt the metadata to the expected format if needed
            for key, value in metadata.items():
                if isinstance(value, dict) and (
                    value["class"] == "File" or value["class"] == "Directory"
                ):
                    metadata[key] = value["path"]

            # Dynamically create a metadata model based on the metadata type
            self.metadata = metadata_models[self.metadata_type](  # noqa
                **{dash_to_snake_case(k): v for k, v in metadata.items()}
            )
        except Exception as e:
            raise ValidationError(f"Failed to load workflow and metadata: {e}") from e

    class Config:
        # Allow arbitrary types to be passed to the model
        arbitrary_types_allowed = True


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

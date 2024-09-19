"""
CLI interface to run a CWL workflow from end to end (production/transformation/job).
"""
from typing import Any, Dict, List

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    Workflow,
)
from pydantic import BaseModel, ConfigDict, field_serializer, field_validator

from dirac_cwl_proto.metadata_models import IMetadataModel

# -----------------------------------------------------------------------------
# Job models
# -----------------------------------------------------------------------------


class JobDescriptionModel(BaseModel):
    """Description of a job."""

    platform: str | None = None
    priority: int = 10
    sites: List[str] | None


class JobParameterModel(BaseModel):
    """Parameter of a job."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sandbox: List[str] | None
    cwl: Dict[str, Any]


class JobMetadataModel(BaseModel):
    """Job metadata."""

    type: str
    # Parameters used to build input/output queries
    # Generally correspond to the inputs of the previous transformations
    query_params: Dict[str, Any] | None = None

    # Validation to ensure type corresponds to a subclass of IMetadataModel
    @field_validator("type")
    def check_type(cls, value):
        # Collect all subclass names of IMetadataModel
        valid_types = {cls.__name__ for cls in IMetadataModel.__subclasses__()}

        # Check if the provided value matches any of the subclass names
        if value not in valid_types:
            raise ValueError(
                f"Invalid type '{value}'. Must be one of: {', '.join(valid_types)}."
            )

        return value


class JobSubmissionModel(BaseModel):
    """Job definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow
    parameters: List[JobParameterModel] | None = None
    description: JobDescriptionModel
    metadata: JobMetadataModel

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Transformation models
# -----------------------------------------------------------------------------


class TransformationMetadataModel(JobMetadataModel):
    """Transformation metadata."""

    # Number of data to group together in a transformation
    # Key: input name, Value: group size
    group_size: Dict[str, int] | None = None


class TransformationSubmissionModel(BaseModel):
    """Transformation definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow
    metadata: TransformationMetadataModel
    description: JobDescriptionModel

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Production models
# -----------------------------------------------------------------------------


class ProductionMetadataModel(BaseModel):
    """Transformation Input Metadata for a transformation."""

    # Number of data to group together in a transformation
    # Key: input name, Value: group size
    group_size: Dict[str, int]


# TODO: workflow should be a Workflow, not a CommandLineTool
# TODO: inputs should not be composed of relative path(?)
class ProductionSubmissionModel(BaseModel):
    """Production definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow
    # Key: step name, Value: metadata
    metadata: Dict[str, ProductionMetadataModel] | None = None
    description: JobDescriptionModel

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")

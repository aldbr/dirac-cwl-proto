"""
CLI interface to run a CWL workflow from end to end (production/transformation/job).
"""

from __future__ import annotations

from typing import Any, Mapping

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    Workflow,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    field_serializer,
    field_validator,
    model_validator,
)

from dirac_cwl_proto.metadata import instantiate_metadata, list_registered
from dirac_cwl_proto.metadata_models import IMetadataModel

# -----------------------------------------------------------------------------
# Job models
# -----------------------------------------------------------------------------


class JobDescriptionModel(BaseModel):
    """Description of a job."""

    platform: str | None = None
    priority: int = 10
    sites: list[str] | None = None


class JobParameterModel(BaseModel):
    """Parameter of a job."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sandbox: list[str] | None
    cwl: dict[str, Any]

    @field_serializer("cwl")
    def serialize_cwl(self, value):
        return save(value)


class JobMetadataModel(BaseModel):
    """Job metadata."""

    type: str = "User"
    # Parameters used to build input/output queries
    # Generally correspond to the inputs of the previous transformations
    query_params: dict[str, Any] = {}

    # Validation to ensure type corresponds to a subclass of IMetadataModel
    @field_validator("type")
    def check_type(cls, value):
        # Validate type against the registry so downstream projects can extend
        valid_types = list_registered()
        if value not in valid_types:
            raise ValueError(f"Invalid type '{value}'. Must be one of: {', '.join(valid_types)}.")

        return value

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> "JobMetadataModel":
        if update is None:
            update = {}
        else:
            update = dict(update)

        # Handle merging of query_params
        if "query_params" in update:
            new_query_params = self.query_params.copy()
            new_query_params.update(update.pop("query_params"))
            update["query_params"] = new_query_params

        return super().model_copy(update=update, deep=deep)

    def to_runtime(self, submitted: "JobSubmissionModel" | None = None) -> IMetadataModel:
        """Instantiate the runtime metadata object from this serializable descriptor.

        If a `submitted` JobSubmissionModel is provided, build the params using
        the task inputs and the first parameter set, then merge with
        `query_params` (this preserves the old behaviour of `_get_metadata`).

        Otherwise fall back to using `query_params` only.
        """

        # Quick helper to convert dash-case to snake_case without importing utils
        def _dash_to_snake(s: str) -> str:
            return s.replace("-", "_")

        if submitted is None:
            return instantiate_metadata(self.type, self.query_params)

        # Build inputs from task defaults and parameter overrides
        inputs: dict[str, Any] = {}
        for inp in submitted.task.inputs:
            input_name = inp.id.split("#")[-1].split("/")[-1]
            input_value = getattr(inp, "default", None)
            params_list = getattr(submitted, "parameters", None)
            if params_list and params_list[0]:
                input_value = params_list[0].cwl.get(input_name, input_value)
            inputs[input_name] = input_value

        # Merge with explicit query params
        if self.query_params:
            inputs.update(self.query_params)

        params = {_dash_to_snake(key): value for key, value in inputs.items()}

        return instantiate_metadata(self.type, params)


class JobSubmissionModel(BaseModel):
    """Job definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow
    parameters: list[JobParameterModel] | None = None
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
    group_size: dict[str, int] | None = None


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


class ProductionStepMetadataModel(BaseModel):
    """Step metadata for a transformation."""

    description: JobDescriptionModel
    metadata: TransformationMetadataModel


class ProductionSubmissionModel(BaseModel):
    """Production definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: Workflow
    # Key: step name, Value: description & metadata of a transformation
    steps_metadata: dict[str, ProductionStepMetadataModel]

    @model_validator(mode="before")
    def validate_steps_metadata(cls, values):
        task = values.get("task")
        steps_metadata = values.get("steps_metadata")

        if task and steps_metadata:
            # Extract the available steps in the task
            task_steps = set([step.id.split("#")[-1] for step in task.steps])
            metadata_keys = set(steps_metadata.keys())

            # Check if all metadata keys exist in the task's workflow steps
            missing_steps = metadata_keys - task_steps
            if missing_steps:
                raise ValueError(f"The following steps are missing from the task workflow: {missing_steps}")

        return values

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")

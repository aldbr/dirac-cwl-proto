"""
Enhanced submission models for DIRAC CWL integration.

This module provides improved submission models with proper separation of concerns,
modern Python typing, and comprehensive numpydoc documentation.
"""

from __future__ import annotations

from typing import Any

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    Workflow,
)
from pydantic import BaseModel, ConfigDict, field_serializer, model_validator

from dirac_cwl_proto.metadata import (
    ExecutionHooksHint,
    SchedulingHint,
)

# -----------------------------------------------------------------------------
# Job models
# -----------------------------------------------------------------------------


class JobParameterModel(BaseModel):
    """Parameter of a job."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sandbox: list[str] | None
    cwl: dict[str, Any]

    @field_serializer("cwl")
    def serialize_cwl(self, value):
        return save(value)


class JobSubmissionModel(BaseModel):
    """Job definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow | ExpressionTool
    parameters: list[JobParameterModel] | None = None
    scheduling: SchedulingHint
    metadata: ExecutionHooksHint

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Transformation models
# -----------------------------------------------------------------------------


class TransformationMetadataModel(ExecutionHooksHint):
    """Transformation metadata."""

    # Number of data to group together in a transformation
    # Key: input name, Value: group size
    group_size: dict[str, int] | None = None


class TransformationSubmissionModel(BaseModel):
    """Transformation definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow | ExpressionTool
    metadata: TransformationMetadataModel
    scheduling: SchedulingHint

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Production models
# -----------------------------------------------------------------------------


class ProductionStepMetadataModel(BaseModel):
    """Step metadata for a transformation."""

    scheduling: SchedulingHint
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
                raise ValueError(
                    f"The following steps are missing from the task workflow: {missing_steps}"
                )

        return values

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (ExpressionTool, CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Module helpers
# -----------------------------------------------------------------------------


def extract_dirac_hints(cwl: Any) -> tuple[ExecutionHooksHint, SchedulingHint]:
    """Thin wrapper that returns (ExecutionHooksHint, SchedulingHint).

    Prefer the class-factory APIs `ExecutionHooksHint.from_cwl` and
    `SchedulingHint.from_cwl` for new code. This helper remains for
    convenience.
    """
    return ExecutionHooksHint.from_cwl(cwl), SchedulingHint.from_cwl(cwl)

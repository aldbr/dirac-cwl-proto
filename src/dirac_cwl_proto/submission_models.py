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

from dirac_cwl_proto.execution_hooks import (
    ExecutionHooksHint,
    SchedulingHint,
    TransformationExecutionHooksHint,
)

# -----------------------------------------------------------------------------
# Job models
# -----------------------------------------------------------------------------


class JobInputModel(BaseModel):
    """Input data and sandbox files for a job execution."""

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
    parameters: list[JobInputModel] | None = None

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")

    @model_validator(mode="before")
    def validate_hints(cls, values):
        task = values.get("task")
        ExecutionHooksHint.from_cwl(task), SchedulingHint.from_cwl(task)
        return values


# -----------------------------------------------------------------------------
# Transformation models
# -----------------------------------------------------------------------------


class TransformationSubmissionModel(BaseModel):
    """Transformation definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow | ExpressionTool

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")

    @model_validator(mode="before")
    def validate_hints(cls, values):
        task = values.get("task")
        TransformationExecutionHooksHint.from_cwl(task), SchedulingHint.from_cwl(task)
        return values


# -----------------------------------------------------------------------------
# Production models
# -----------------------------------------------------------------------------


class ProductionSubmissionModel(BaseModel):
    """Production definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: Workflow

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (ExpressionTool, CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")

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
    DataManager,
    JobExecutor,
)

# Local imports

# -----------------------------------------------------------------------------
# Task models
# -----------------------------------------------------------------------------


class TaskDescriptionModel(JobExecutor):
    """
    Description of a task (job/transformation/production step).

    This class extends the core JobExecutor with additional methods
    for CWL integration and backward compatibility.

    Parameters
    ----------
    platform : str | None
        Target platform name (for example ``"DIRAC"``). If ``None``, no
        platform preference is encoded.
    priority : int, optional
        Scheduling priority. Higher values indicate higher priority.
        Defaults to ``10``.
    sites : list[str] | None
        Optional list of candidate site names where the task may run.

    Notes
    -----
    This is a serialisable Pydantic descriptor intended to carry CWL hints
    related to runtime placement and scheduling. Prefer using the class
    factory ``TaskDescriptionModel.from_hints(cwl)`` when extracting hints
    from parsed CWL objects.
    """

    @classmethod
    def from_hints(cls, cwl: Any) -> "TaskDescriptionModel":
        """
        Create a ``TaskDescriptionModel`` from CWL hints.

        Parameters
        ----------
        cwl : Any
            A parsed CWL ``CommandLineTool`` or ``Workflow`` object (for
            example from ``cwl-utils``).

        Returns
        -------
        TaskDescriptionModel
            Descriptor populated from CWL hints. Unknown or missing hints
            are ignored and sensible defaults are used.
        """
        return cls.from_cwl_hints(cwl)


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
    description: TaskDescriptionModel
    metadata: DataManager

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Transformation models
# -----------------------------------------------------------------------------


class TransformationMetadataModel(DataManager):
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
    description: TaskDescriptionModel

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

    description: TaskDescriptionModel
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
        if isinstance(value, (ExpressionTool, CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Module helpers
# -----------------------------------------------------------------------------


def extract_dirac_hints(cwl: Any) -> tuple[DataManager, TaskDescriptionModel]:
    """Thin wrapper that returns (DataManager, TaskDescriptionModel).

    Prefer the class-factory APIs `DataManager.from_hints` and
    `TaskDescriptionModel.from_hints` for new code. This helper remains for
    convenience.
    """
    return DataManager.from_hints(cwl), TaskDescriptionModel.from_hints(cwl)

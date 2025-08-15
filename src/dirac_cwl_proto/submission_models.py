"""
CLI interface to run a CWL workflow from end to end (production/transformation/job).
"""

from __future__ import annotations

from typing import Any, Mapping

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
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
# Task models
# -----------------------------------------------------------------------------


class TaskDescriptionModel(BaseModel):
    """
    Description of a task (job/transformation/production step).

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

    platform: str | None = None
    priority: int = 10
    sites: list[str] | None = None

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
        description = cls()

        hints = cwl.hints or []
        for hint in hints:
            if hint.get("class") == "dirac:description":
                hint_body = {k: v for k, v in hint.items() if k != "class"}
                description = description.model_copy(update=hint_body)

        return description


class TaskMetadataModel(BaseModel):
    """
    Task metadata descriptor used by Jobs, Transformations and Production
    steps.

    Parameters
    ----------
    type : str
        Registry key identifying the runtime metadata implementation to
        instantiate (for example ``"User"``). The concrete runtime type
        must be registered with the metadata registry for ``to_runtime`` to
        successfully instantiate it.
    query_params : dict[str, Any], optional
        Parameters used to build input/output queries. These are merged with
        task defaults and any runtime parameter overrides when
        ``to_runtime`` is called.
    group_size : int | None
        Optional grouping size used by some transformation metadata
        implementations.

    Methods
    -------
    model_copy(update=None, deep=False)
        Return a copy of the Pydantic model, optionally applying updates.
    to_runtime(submitted)
        Build runtime instantiation parameters from the submission context
        (task inputs + parameter overrides + ``query_params``) and
        instantiate a concrete :class:`IMetadataModel` via the metadata
        registry.
    from_hints(cwl)
        Class factory extracting metadata from CWL hints.

    Notes
    -----
    - Unknown CWL hints are ignored by the ``from_hints`` factory.
    - During ``to_runtime``, query parameter keys are converted from
      dash-case (e.g. ``"some-key"``) to snake_case (``"some_key"``) to
      match typical Python argument names used by runtime implementations.
    """

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
    ) -> "TaskMetadataModel":
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
        """
        Build and instantiate the runtime metadata implementation.

        The returned object is an instance of :class:`IMetadataModel` created
        by the :mod:`dirac_cwl_proto.metadata` registry. The instantiation
        parameters are constructed by merging, in order:

        1. Input defaults declared on the CWL task (if ``submitted`` is
           provided).
        2. The first set of CWL parameter overrides (``submitted.parameters``),
           if present.
        3. The descriptor's ``query_params``.

        During merging, keys are normalised from dash-case to snake_case to
        align with typical Python argument names used by runtime
        implementations.

        Parameters
        ----------
        submitted : JobSubmissionModel | None
            Optional submission context used to resolve CWL input defaults
            and parameter overrides.

        Returns
        -------
        IMetadataModel
            Runtime metadata implementation instantiated from the registry.
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

    @classmethod
    def from_hints(cls, cwl: Any) -> "TaskMetadataModel":
        """
        Create a ``TaskMetadataModel`` from CWL hints.

        Parameters
        ----------
        cwl : Any
            A parsed CWL ``CommandLineTool`` or ``Workflow`` object.

        Returns
        -------
        TaskMetadataModel
            Descriptor populated from CWL hints; unknown hints are ignored.
        """
        metadata = cls()

        hints = cwl.hints or []
        for hint in hints:
            hint_class = hint.get("class")
            hint_body = {k: v for k, v in hint.items() if k != "class"}

            if hint_class == "dirac:metadata":
                metadata = metadata.model_copy(update=hint_body)

        return metadata


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
    metadata: TaskMetadataModel

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Transformation models
# -----------------------------------------------------------------------------


class TransformationMetadataModel(TaskMetadataModel):
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
        if isinstance(value, (CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Module helpers
# -----------------------------------------------------------------------------


def extract_dirac_hints(cwl: Any) -> tuple[TaskMetadataModel, TaskDescriptionModel]:
    """Thin wrapper that returns (TaskMetadataModel, TaskDescriptionModel).

    Prefer the class-factory APIs `TaskMetadataModel.from_hints` and
    `TaskDescriptionModel.from_hints` for new code. This helper remains for
    convenience and backward compatibility.
    """
    return TaskMetadataModel.from_hints(cwl), TaskDescriptionModel.from_hints(cwl)

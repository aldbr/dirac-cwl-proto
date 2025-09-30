"""Core metadata framework for DIRAC CWL integration.

This module provides the foundational classes and interfaces for the extensible
metadata plugin system in DIRAC/DIRACX.
"""

from __future__ import annotations

import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Mapping, Optional, TypeVar, Union, cast

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    Saveable,
    Workflow,
)
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from ruamel.yaml import YAML

logger = logging.getLogger(__name__)

# TypeVar for generic class methods
T = TypeVar("T", bound="SchedulingHint")


class DataCatalogInterface(ABC):
    """Abstract interface for data catalog operations."""

    @abstractmethod
    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Generate input data query.

        Parameters
        ----------
        input_name : str
            Name of the input parameter.
        **kwargs : Any
            Additional query parameters.

        Returns
        -------
        Union[Path, List[Path], None]
            Path(s) to input data or None if not found.
        """
        pass

    @abstractmethod
    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Generate output data path.

        Parameters
        ----------
        output_name : str
            Name of the output parameter.

        Returns
        -------
        Optional[Path]
            Path where output should be stored or None.
        """
        ...

    def store_output(self, output_name: str, src_path: str, **kwargs: Any) -> None:
        """Store output in the data catalog.

        Parameters
        ----------
        output_name : str
            Name of the output parameter.
        src_path : str | Path
            Source path of the output file.
        """
        output_path = self.get_output_query(output_name, **kwargs)
        if not output_path:
            raise RuntimeError(f"No output path defined for {output_name}")

        output_path.mkdir(exist_ok=True, parents=True)
        dest = output_path / Path(src_path).name
        Path(src_path).rename(dest)
        logger.info(f"Output {output_name} stored in {dest}")


class DummyDataCatalogInterface(DataCatalogInterface):
    """Default implementation that returns None for all queries.

    This is used as the default data catalog when no specific implementation
    is provided by a plugin.
    """

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Return None - no input data available."""
        return None

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Return None - no output path available."""
        return None


class ExecutionHooksBasePlugin(BaseModel):
    """Base class for all runtime plugin models with execution hooks.

    This class uses composition instead of inheritance for data catalog operations,
    providing better separation of concerns and flexibility.
    """

    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
        arbitrary_types_allowed=True,
        json_schema_extra={
            "title": "DIRAC Metadata Model",
            "description": "Base metadata model for DIRAC jobs",
        },
    )

    # Class-level metadata for plugin discovery
    vo: ClassVar[Optional[str]] = None
    version: ClassVar[str] = "1.0.0"
    description: ClassVar[str] = "Base metadata model"

    # Private attribute for data catalog interface - not part of Pydantic model validation
    _data_catalog: DataCatalogInterface = PrivateAttr(
        default_factory=DummyDataCatalogInterface
    )

    @property
    def data_catalog(self) -> DataCatalogInterface:
        """Get the data catalog interface."""
        return self._data_catalog

    @data_catalog.setter
    def data_catalog(self, value: DataCatalogInterface) -> None:
        """Set the data catalog interface."""
        self._data_catalog = value

    def __init__(self, **data):
        """Initialize with data catalog interface."""
        super().__init__(**data)
        # Data catalog will be set by subclasses as needed

    @classmethod
    def name(cls) -> str:
        """Auto-derive hook plugin identifier from class name."""
        return cls.__name__

    def download_lfns(self, inputs: Any, job_path: Path) -> dict[str, Path]:
        new_paths: dict[str, Path] = {}
        if inputs.lfns_input:
            for input_name, lfn in inputs.lfns_input.items():
                # TODO: use data catalog interface
                if isinstance(lfn, Path):
                    input_path = lfn
                else:
                    input_path = Path(lfn)
                shutil.copy(input_path, job_path / input_path.name)
                new_paths[input_name] = Path(input_path.name)
        return new_paths

    def update_inputs(self, inputs: Any, updates: dict[str, Path]):
        for input_name, path in updates.items():
            inputs.cwl[input_name] = path

    def pre_process(
        self,
        executable: CommandLineTool | Workflow | ExpressionTool,
        arguments: Any | None,
        job_path: Path,
        command: List[str],
        **kwargs: Any,
    ) -> List[str]:
        """Pre-process job inputs and command.

        Parameters
        ----------
        job_path : Path
            Path to the job working directory.
        command : List[str]
            The command to be executed.

        Returns
        -------
        List[str]
            Modified command list.
        """
        if arguments:
            updates = self.download_lfns(arguments, job_path)
            self.update_inputs(arguments, updates)

            parameter_dict = save(cast(Saveable, arguments.cwl))
            parameter_path = job_path / "parameter.cwl"
            with open(parameter_path, "w") as parameter_file:
                YAML().dump(parameter_dict, parameter_file)
            command.append(str(parameter_path.name))
        return command

    def post_process(self, job_path: Path, **kwargs: Any) -> bool:
        """Post-process job outputs.

        Parameters
        ----------
        job_path : Path
            Path to the job working directory.
        """
        return True

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Delegate to data catalog interface."""
        return self.data_catalog.get_input_query(input_name, **kwargs)

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Delegate to data catalog interface."""
        return self.data_catalog.get_output_query(output_name, **kwargs)

    def store_output(self, output_name: str, src_path: str, **kwargs: Any) -> None:
        """Delegate to data catalog interface."""
        self.data_catalog.store_output(output_name, src_path, **kwargs)

    @classmethod
    def get_schema_info(cls) -> Dict[str, Any]:
        """Get schema information for this metadata model."""
        return {
            "hook_plugin": cls.name(),
            "vo": cls.vo,
            "version": cls.version,
            "description": cls.description,
            "schema": cls.model_json_schema(),
        }


class Hint(ABC):
    """Base class for all DIRAC hints and requirements models."""

    @classmethod
    @abstractmethod
    def from_cwl(cls, cwl_object: Any) -> "Hint":
        """Extract hint information from a CWL object."""
        pass


class SchedulingHint(BaseModel, Hint):
    """Descriptor for job execution configuration."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    platform: Optional[str] = Field(
        default=None, description="Target platform (e.g., 'DIRAC', 'DIRACX')"
    )

    priority: int = Field(
        default=10, description="Job priority (higher values = higher priority)"
    )

    sites: Optional[List[str]] = Field(
        default=None, description="Candidate execution sites"
    )

    @classmethod
    def from_cwl(cls: type[T], cwl_object: Any) -> T:
        """Extract task descriptor from CWL hints."""
        descriptor = cls()

        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:scheduling":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                descriptor = descriptor.model_copy(update=hint_data)

        return descriptor


class ExecutionHooksHint(BaseModel, Hint):
    """Descriptor for data management configuration in CWL hints.

    This class represents the serializable data management configuration that
    can be embedded in CWL hints and later instantiated into concrete
    metadata models.

    Enhanced with submission functionality for DIRAC CWL integration.
    """

    model_config = ConfigDict(
        extra="allow",  # Allow vo-specific fields
        validate_assignment=True,
        json_schema_extra={
            "title": "DIRAC Data Manager",
            "description": "Data management configuration for DIRAC jobs",
        },
    )

    hook_plugin: str = Field(
        default="UserPlugin",
        description="Registry key for the metadata implementation class",
    )

    # Enhanced fields for submission functionality
    configuration: Dict[str, Any] = Field(
        default_factory=dict, description="Additional parameters for metadata plugins"
    )

    def model_copy(
        self,
        update: Optional[Mapping[str, Any]] = None,
        *,
        deep: bool = False,
    ) -> "ExecutionHooksHint":
        """Enhanced model copy with intelligent merging of dict fields (including configuration)."""
        if update is None:
            update = {}
        else:
            update = dict(update)

        merged_update = {}
        for key, value in update.items():
            if (
                hasattr(self, key)
                and isinstance(getattr(self, key), dict)
                and isinstance(value, dict)
            ):
                existing_value = getattr(self, key).copy()
                existing_value.update(value)
                merged_update[key] = existing_value
            else:
                merged_update[key] = value

        return super().model_copy(update=merged_update, deep=deep)

    def to_runtime(self, submitted: Optional[Any] = None) -> "ExecutionHooksBasePlugin":
        """
            Build and instantiate the runtime metadata implementation.

        The returned object is an instance of :class:`ExecutionHooksBasePlugin` created
        by the metadata registry. The instantiation parameters are constructed
            by merging, in order:

            1. Input defaults declared on the CWL task (if ``submitted`` is provided).
            2. The first set of CWL parameter overrides (``submitted.parameters``),
               if present.
            3. The descriptor's ``configuration``.

            During merging, keys are normalised from dash-case to snake_case to
            align with typical Python argument names used by runtime implementations.

            Parameters
            ----------
            submitted : JobSubmissionModel | None
                Optional submission context used to resolve CWL input defaults
                and parameter overrides.

            Returns
            -------
            ExecutionHooksBasePlugin
                Runtime plugin implementation instantiated from the registry.
        """
        # Import here to avoid circular imports
        from .registry import get_registry

        # Quick helper to convert dash-case to snake_case without importing utils
        def _dash_to_snake(s: str) -> str:
            return s.replace("-", "_")

        if submitted is None:
            descriptor = ExecutionHooksHint(
                hook_plugin=self.hook_plugin, **self.configuration
            )
            return get_registry().instantiate_plugin(descriptor)

        # Build inputs from task defaults and parameter overrides
        inputs: dict[str, Any] = {}
        for inp in submitted.task.inputs:
            input_name = inp.id.split("#")[-1].split("/")[-1]
            input_value = getattr(inp, "default", None)
            params_list = getattr(submitted, "parameters", None)
            if params_list and params_list[0]:
                input_value = params_list[0].cwl.get(input_name, input_value)
            inputs[input_name] = input_value

        # Merge with explicit configuration
        if self.configuration:
            inputs.update(self.configuration)

        params = {_dash_to_snake(key): value for key, value in inputs.items()}

        descriptor = ExecutionHooksHint(hook_plugin=self.hook_plugin, **params)
        return get_registry().instantiate_plugin(descriptor)

    @classmethod
    def from_cwl(cls, cwl_object: Any) -> "ExecutionHooksHint":
        """Extract metadata descriptor from CWL object using Hint interface."""
        descriptor = cls()
        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:execution-hooks":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                descriptor = descriptor.model_copy(update=hint_data)
        return descriptor


class TransformationExecutionHooksHint(ExecutionHooksHint):
    """Extended data manager for transformations."""

    group_size: Optional[Dict[str, int]] = Field(
        default=None, description="Input grouping configuration for transformation jobs"
    )

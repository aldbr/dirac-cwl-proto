"""Core metadata framework for DIRAC CWL integration.

This module provides the foundational classes and interfaces for the extensible
metadata plugin system in DIRAC/DIRACX.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Mapping, Optional, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

# TypeVar for generic class methods
T = TypeVar("T", bound="TaskDescriptor")


class MetadataProcessor(ABC):
    """Abstract base class for metadata processing hooks.

    This class defines the interface for pre/post processing operations
    that can be performed during job execution.
    """

    @abstractmethod
    def pre_process(self, job_path: Path, command: List[str]) -> List[str]:
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
        pass

    @abstractmethod
    def post_process(self, job_path: Path) -> bool:
        """Post-process job outputs.

        Parameters
        ----------
        job_path : Path
            Path to the job working directory.

        Returns
        -------
        bool
            True if post-processing succeeded, False otherwise.
        """
        pass


class DataCatalogInterface(ABC):
    """Abstract interface for data catalog operations."""

    @abstractmethod
    def get_input_query(self, input_name: str, **kwargs: Any) -> Union[Path, List[Path], None]:
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
    def get_output_query(self, output_name: str) -> Optional[Path]:
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
        pass

    def store_output(self, output_name: str, src_path: str) -> None:
        """Store output in the data catalog.

        Parameters
        ----------
        output_name : str
            Name of the output parameter.
        src_path : str
            Source path of the output file.
        """
        output_path = self.get_output_query(output_name)
        if not output_path:
            raise RuntimeError(f"No output path defined for {output_name}")

        output_path.mkdir(exist_ok=True, parents=True)
        dest = output_path / Path(src_path).name
        Path(src_path).rename(dest)
        logger.info(f"Output {output_name} stored in {dest}")


class BaseMetadataModel(BaseModel, DataCatalogInterface, MetadataProcessor):
    """Base class for all metadata models.

    This class combines Pydantic validation with the metadata processing
    interfaces to provide a complete foundation for metadata implementations.
    """

    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
        arbitrary_types_allowed=True,
        json_schema_extra={"title": "DIRAC Metadata Model", "description": "Base metadata model for DIRAC jobs"},
    )

    # Class-level metadata for plugin discovery
    metadata_type: ClassVar[str] = "base"
    experiment: ClassVar[Optional[str]] = None
    version: ClassVar[str] = "1.0.0"
    description: ClassVar[str] = "Base metadata model"

    def pre_process(self, job_path: Path, command: List[str]) -> List[str]:
        """Default pre-processing: return command unchanged."""
        return command

    def post_process(self, job_path: Path) -> bool:
        """Default post-processing: always succeed."""
        return True

    def get_input_query(self, input_name: str, **kwargs: Any) -> Union[Path, List[Path], None]:
        """Default input query: return None."""
        return None

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Default output query: return None."""
        return None

    @classmethod
    def get_schema_info(cls) -> Dict[str, Any]:
        """Get schema information for this metadata model."""
        return {
            "type": cls.metadata_type,
            "experiment": cls.experiment,
            "version": cls.version,
            "description": cls.description,
            "schema": cls.model_json_schema(),
        }


class MetadataDescriptor(BaseModel):
    """Descriptor for metadata configuration in CWL hints.

    This class represents the serializable metadata configuration that
    can be embedded in CWL hints and later instantiated into concrete
    metadata models.

    Enhanced with submission functionality for DIRAC CWL integration.
    """

    model_config = ConfigDict(
        extra="allow",  # Allow experiment-specific fields
        validate_assignment=True,
        json_schema_extra={
            "title": "DIRAC Metadata Descriptor",
            "description": "Metadata configuration for DIRAC jobs",
        },
    )

    metadata_class: str = Field(default="User", description="Registry key for the metadata implementation class")

    experiment: Optional[str] = Field(default=None, description="Experiment namespace (e.g., 'lhcb', 'ctao')")

    version: Optional[str] = Field(default=None, description="Metadata model version")

    # Enhanced fields for submission functionality
    type: str = Field(default="User", description="Legacy field for backward compatibility (alias for metadata_class)")
    query_params: Dict[str, Any] = Field(default_factory=dict, description="Additional parameters for metadata plugins")

    def __init__(self, **data):
        """Initialize with metadata_class/type field synchronization."""
        # Synchronize metadata_class and type fields before calling super()
        if "type" in data and "metadata_class" not in data:
            data["metadata_class"] = data["type"]
        elif "metadata_class" in data and "type" not in data:
            data["type"] = data["metadata_class"]
        super().__init__(**data)

    def model_copy_with_merge(
        self,
        *,
        update: Optional[Dict[str, Any]] = None,
        deep: bool = False,
    ) -> "MetadataDescriptor":
        """Create a copy with intelligent merging of nested dictionaries."""
        if update is None:
            return self.model_copy(deep=deep)

        # Handle nested dictionary merging for experiment-specific fields
        merged_update = {}
        for key, value in update.items():
            if hasattr(self, key) and isinstance(getattr(self, key), dict) and isinstance(value, dict):
                existing_value = getattr(self, key).copy()
                existing_value.update(value)
                merged_update[key] = existing_value
            else:
                merged_update[key] = value

        return self.model_copy(update=merged_update, deep=deep)

    def model_copy(
        self,
        update: Optional[Mapping[str, Any]] = None,
        *,
        deep: bool = False,
    ) -> "MetadataDescriptor":
        """Enhanced model copy with intelligent merging of query_params."""
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

    def to_runtime(self, submitted: Optional[Any] = None) -> "BaseMetadataModel":
        """
        Build and instantiate the runtime metadata implementation.

        The returned object is an instance of :class:`BaseMetadataModel` created
        by the metadata registry. The instantiation parameters are constructed
        by merging, in order:

        1. Input defaults declared on the CWL task (if ``submitted`` is provided).
        2. The first set of CWL parameter overrides (``submitted.parameters``),
           if present.
        3. The descriptor's ``query_params``.

        During merging, keys are normalised from dash-case to snake_case to
        align with typical Python argument names used by runtime implementations.

        Parameters
        ----------
        submitted : JobSubmissionModel | None
            Optional submission context used to resolve CWL input defaults
            and parameter overrides.

        Returns
        -------
        BaseMetadataModel
            Runtime metadata implementation instantiated from the registry.
        """
        # Import here to avoid circular imports
        from .registry import instantiate_metadata

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
    def from_cwl_hints(cls, cwl_object: Any) -> "MetadataDescriptor":
        """Extract metadata descriptor from CWL hints.

        Parameters
        ----------
        cwl_object : Any
            A parsed CWL object (CommandLineTool, Workflow, etc.)

        Returns
        -------
        MetadataDescriptor
            Extracted metadata descriptor with defaults for missing fields.
        """
        descriptor = cls()

        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:metadata":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                descriptor = descriptor.model_copy_with_merge(update=hint_data)

        return descriptor

    @classmethod
    def from_hints(cls, cwl_object: Any) -> "MetadataDescriptor":
        """
        Create a MetadataDescriptor from CWL hints.

        Alias for from_cwl_hints for backward compatibility.

        Parameters
        ----------
        cwl_object : Any
            A parsed CWL ``CommandLineTool`` or ``Workflow`` object.

        Returns
        -------
        MetadataDescriptor
            Descriptor populated from CWL hints; unknown hints are ignored.
        """
        return cls.from_cwl_hints(cwl_object)


class TransformationMetadataDescriptor(MetadataDescriptor):
    """Extended metadata descriptor for transformations."""

    group_size: Optional[Dict[str, int]] = Field(
        default=None, description="Input grouping configuration for transformation jobs"
    )


class TaskDescriptor(BaseModel):
    """Descriptor for task execution configuration."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    platform: Optional[str] = Field(default=None, description="Target platform (e.g., 'DIRAC', 'DIRACX')")

    priority: int = Field(default=10, description="Job priority (higher values = higher priority)")

    sites: Optional[List[str]] = Field(default=None, description="Candidate execution sites")

    @classmethod
    def from_cwl_hints(cls: type[T], cwl_object: Any) -> T:
        """Extract task descriptor from CWL hints."""
        descriptor = cls()

        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:description":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                descriptor = descriptor.model_copy(update=hint_data)

        return descriptor

"""Core metadata framework for DIRAC CWL integration.

This module provides the foundational classes and interfaces for the extensible
metadata plugin system in DIRAC/DIRACX.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


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
        extra="forbid",
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
    def from_cwl_hints(cls, cwl_object: Any) -> "TaskDescriptor":
        """Extract task descriptor from CWL hints."""
        descriptor = cls()

        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:description":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                descriptor = descriptor.model_copy(update=hint_data)

        return descriptor

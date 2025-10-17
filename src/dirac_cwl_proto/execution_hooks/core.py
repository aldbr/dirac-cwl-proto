"""Core metadata framework for DIRAC CWL integration.

This module provides the foundational classes and interfaces for the extensible
metadata plugin system in DIRAC/DIRACX.
"""

from __future__ import annotations

import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Self,
    TypeVar,
    Union,
    cast,
)

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    File,
    Saveable,
    Workflow,
)
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from ruamel.yaml import YAML

from dirac_cwl_proto.commands import PostProcessCommand, PreProcessCommand
from dirac_cwl_proto.core.exceptions import WorkflowProcessingException

logger = logging.getLogger(__name__)

# TypeVar for generic class methods
T = TypeVar("T", bound="SchedulingHint")


class DataCatalogInterface(ABC):
    """Abstract interface for data catalog operations.

    This interface defines the contract for data discovery and output registration
    in various data management systems. Implementations can range from simple
    filesystem-based catalogs to complex distributed data management systems.

    The interface is designed to be storage-agnostic and allows different
    implementations to handle data in ways appropriate to their underlying
    storage and metadata systems.
    """

    @abstractmethod
    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Resolve input data locations for processing.

        This method provides a mechanism to discover input data based on
        logical names and additional query parameters. The implementation
        determines how to translate logical input names into concrete data
        locations.

        Parameters
        ----------
        input_name : str
            Logical name or identifier for the input data. This serves as
            a documentation label and lookup key for the data catalog.
        **kwargs : Any
            Implementation-specific query parameters that may influence
            data discovery (e.g., version, campaign, site, data_type).

        Returns
        -------
        Union[Path, List[Path], None]
            Resolved data location(s). Returns:
            - Path: Single data location
            - List[Path]: Multiple data locations for the same logical input
            - None: No data found matching the query criteria
        """
        pass

    @abstractmethod
    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Determine where output data should be stored.

        This method generates appropriate storage locations for output data
        based on the output name and catalog configuration. The returned
        location serves as a staging area or final destination for outputs.

        Parameters
        ----------
        output_name : str
            Logical name or identifier for the output data. This serves as
            a documentation label and determines output organization.
        **kwargs : Any
            Implementation-specific parameters that may influence output
            placement (e.g., campaign, site, data classification).

        Returns
        -------
        Optional[Path]
            Designated output location where data should be stored.
            Returns None if no suitable output location can be determined.
        """
        pass

    @abstractmethod
    def store_output(self, output_name: str, **kwargs: Any) -> None:
        """Register or store output data in the catalog.

        This method handles the catalog-specific operations needed to make
        output data available through the data management system. The actual
        storage mechanism is implementation-dependent and may involve file
        operations, database registrations, or API calls to external systems.

        Parameters
        ----------
        output_name : str
            Logical name or identifier for the output data. This serves as
            a documentation label for organizing and retrieving the data.
        **kwargs : Any
            Implementation-specific parameters that provide the necessary
            information for storing the output (e.g., source paths, metadata,
            checksums, file sizes, destination parameters).
        """
        pass


class DefaultDataCatalogInterface(DataCatalogInterface):
    """Default filesystem-based data catalog using Logical File Names (LFNs).

    This provides a simple, filesystem-based implementation suitable for
    examples and testing. Uses a structured LFN path format:
    /vo/campaign/site/data_type/files
    """

    def __init__(
        self,
        vo: Optional[str] = None,
        campaign: Optional[str] = None,
        site: Optional[str] = None,
        data_type: Optional[str] = None,
        base_path: str = "/",
    ):
        self.vo = vo
        self.campaign = campaign
        self.site = site
        self.data_type = data_type
        self.base_path = Path(base_path)

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Generate LFN-based input query path.

        Accepts and ignores extra kwargs for interface compatibility.
        """
        # Build LFN: /base_path/vo/campaign/site/data_type/input_name
        path_parts = []

        if self.vo:
            path_parts.append(self.vo)

        if self.campaign:
            path_parts.append(self.campaign)
        if self.site:
            path_parts.append(self.site)
        if self.data_type:
            path_parts.append(self.data_type)

        if len(path_parts) > 0:  # More than just VO
            return self.base_path / Path(*path_parts) / Path(input_name)

        return self.base_path / Path(input_name)

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Generate LFN-based output path.

        Accepts and ignores extra kwargs for interface compatibility.
        """
        # Output path: /grid/data/vo/outputs/campaign/site
        output_base = self.base_path
        if self.vo:
            output_base = output_base / self.vo
        output_base = output_base / "outputs"

        if self.campaign:
            output_base = output_base / self.campaign
        if self.site:
            output_base = output_base / self.site

        return output_base

    def store_output(self, output_name: str, **kwargs: Any) -> None:
        """Store output file in the filesystem-based catalog.

        This implementation handles filesystem operations to move output files
        to their designated LFN-based storage locations.

        Parameters
        ----------
        output_name : str
            Logical name for the output data.
        **kwargs : Any
            Expected parameters:
            - src_path (str | Path): Source path of the output file to store
            Additional parameters are passed to get_output_query.

        Raises
        ------
        RuntimeError
            If no output path can be determined or if src_path is not provided.
        """
        # Extract the filesystem-specific parameter
        src_path = kwargs.get("src_path")
        if not src_path:
            raise RuntimeError(
                f"src_path parameter required for filesystem storage of {output_name}"
            )

        # Get the output directory
        output_path = self.get_output_query(output_name, **kwargs)
        if not output_path:
            raise RuntimeError(f"No output path defined for {output_name}")

        # Ensure output directory exists
        output_path.mkdir(exist_ok=True, parents=True)

        # Move file to destination
        src_path_obj = Path(src_path)
        dest = output_path / src_path_obj.name
        src_path_obj.rename(dest)
        logger.info(f"Output {output_name} stored in {dest}")


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
    _data_catalog: Optional[DataCatalogInterface] = PrivateAttr(
        default_factory=lambda: DefaultDataCatalogInterface()
    )

    _preprocess_commands: List[type[PreProcessCommand]] = PrivateAttr(default=[])
    _postprocess_commands: List[type[PostProcessCommand]] = PrivateAttr(default=[])

    @property
    def data_catalog(self) -> Optional[DataCatalogInterface]:
        """Get the data catalog interface."""
        return self._data_catalog

    @data_catalog.setter
    def data_catalog(self, value: DataCatalogInterface) -> None:
        """Set the data catalog interface."""
        self._data_catalog = value

    @property
    def preprocess_commands(self) -> List[type[PreProcessCommand]]:
        """Get the list of pre-processing commands."""
        return self._preprocess_commands

    @preprocess_commands.setter
    def preprocess_commands(self, value: List[type[PreProcessCommand]]) -> None:
        """Set the list of pre-processing commands."""
        self._preprocess_commands = value

    @property
    def postprocess_commands(self) -> List[type[PostProcessCommand]]:
        """Get the list of post-processing commands."""
        return self._postprocess_commands

    @postprocess_commands.setter
    def postprocess_commands(self, value: List[type[PostProcessCommand]]) -> None:
        """Set the list of post-processing commands."""
        self._postprocess_commands = value

    def __init__(self, **data):
        """Initialize with data catalog interface."""
        super().__init__(**data)
        # Data catalog will be set by subclasses as needed

    @classmethod
    def name(cls) -> str:
        """Auto-derive hook plugin identifier from class name."""
        return cls.__name__

    def download_lfns(
        self, inputs: Any, job_path: Path
    ) -> dict[str, Path | list[Path]]:
        # TODO: use data catalog interface
        new_paths: dict[str, Path | list[Path]] = {}
        if inputs.lfns_input:
            for input_name, lfns in inputs.lfns_input.items():
                if not isinstance(lfns, list):
                    lfns = [lfns]
                paths = []
                for lfn in lfns:
                    if isinstance(lfn, Path):
                        input_path = Path(str(lfn).removeprefix("lfn:"))
                    else:
                        input_path = Path(lfn.removeprefix("lfn:"))
                    shutil.copy(input_path, job_path / input_path.name)
                    paths.append(Path(input_path.name))
                if paths:
                    new_paths[input_name] = paths
        return new_paths

    def update_inputs(self, inputs: Any, updates: dict[str, Path | list[Path]]):
        for input_name, path in updates.items():
            if isinstance(path, Path):
                inputs.cwl[input_name] = File(path=str(path))
            else:
                inputs.cwl[input_name] = []
                for p in path:
                    inputs.cwl[input_name].append(File(path=str(p)))

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
        for preprocess_command in self.preprocess_commands:
            if not issubclass(preprocess_command, PreProcessCommand):
                msg = f"The command {preprocess_command} is not a {PreProcessCommand.__name__}"
                logger.error(msg)
                raise TypeError(msg)

            try:
                preprocess_command().execute(job_path, **kwargs)
            except Exception as e:
                msg = f"Command '{preprocess_command.__name__}' failed during the pre-process stage: {e}"
                logger.exception(msg)
                raise WorkflowProcessingException(msg) from e

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
        for postprocess_command in self.postprocess_commands:
            if not issubclass(postprocess_command, PostProcessCommand):
                msg = f"The command {postprocess_command} is not a {PostProcessCommand.__name__}"
                logger.error(msg)
                raise TypeError(msg)

            try:
                postprocess_command().execute(job_path, **kwargs)
            except Exception as e:
                msg = f"Command '{postprocess_command.__name__}' failed during the post-process stage: {e}"
                logger.exception(msg)
                raise WorkflowProcessingException(msg) from e

        return True

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Delegate to data catalog interface."""
        if self.data_catalog is None:
            return None
        return self.data_catalog.get_input_query(input_name, **kwargs)

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Delegate to data catalog interface."""
        if self.data_catalog is None:
            return None
        return self.data_catalog.get_output_query(output_name, **kwargs)

    def store_output(self, output_name: str, src_path: str, **kwargs: Any) -> None:
        """Delegate to data catalog interface.

        This method provides backward compatibility by forwarding the src_path
        parameter through kwargs to the data catalog implementation.
        """
        if self.data_catalog is None:
            logger.warning(
                f"No data catalog available, cannot store output {output_name}"
            )
            return
        # Forward src_path through kwargs to maintain interface compatibility
        kwargs["src_path"] = src_path
        self.data_catalog.store_output(output_name, **kwargs)

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
            if hint.get("class") == "dirac:Scheduling":
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
        default="QueryBasedPlugin",
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
    ) -> Self:
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

            During merging, keys are normalized from dash-case to snake_case to
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
    def from_cwl(cls, cwl_object: Any) -> Self:
        """Extract metadata descriptor from CWL object using Hint interface."""
        descriptor = cls()
        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:ExecutionHooks":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                descriptor = descriptor.model_copy(update=hint_data)
        return descriptor


class TransformationExecutionHooksHint(ExecutionHooksHint):
    """Extended data manager for transformations."""

    group_size: Optional[Dict[str, int]] = Field(
        default=None, description="Input grouping configuration for transformation jobs"
    )

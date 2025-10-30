"""Core metadata framework for DIRAC CWL integration.

This module provides the foundational classes and interfaces for the extensible
metadata plugin system in DIRAC/DIRACX.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
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

from dirac_cwl_proto.execution_hooks.DataManagement.DataManager import DataManager
from dirac_cwl_proto.execution_hooks.DataManagement.Sandbox import SandboxStoreClient

logger = logging.getLogger(__name__)

# TypeVar for generic class methods
T = TypeVar("T", bound="SchedulingHint")


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

    _datamanager: DataManager = PrivateAttr(
        default_factory=lambda: DataManager(file_catalog="LocalFileCatalog")
    )

    @classmethod
    def name(cls) -> str:
        """Auto-derive hook plugin identifier from class name."""
        return cls.__name__

    def download_lfns(
        self, inputs: Any, job_path: Path
    ) -> dict[str, Path | list[Path]]:
        """Download LFNs into the job working directory.

        This method retrieves files referenced by LFNs from the job inputs
        and copies them into the specified working directory. The LFNs are
        expected to follow the format ``lfn:<path>``, which is resolved to
        a local path. Each downloaded file path is then returned and can be
        used to update CWL job inputs accordingly.

        Parameters
        ----------
        inputs : JobInputModel
            The job input model containing ``lfns_input``, a mapping from input names to one or more LFN paths.
        job_path : Path
            Path to the job working directory where files will be copied.

        Returns
        -------
        dict[str, Path | list[Path]]
            A dictionary mapping each input name to the corresponding downloaded
            file path(s) located in the working directory.

        Notes
        -----
        - Currently, this method performs a local copy of files and does not
          use a remote data catalog or storage service.
        - The returned paths are relative to the job working directory.
        """
        new_paths: dict[str, Path | list[Path]] = {}
        if inputs.lfns_input:
            for input_name, lfns in inputs.lfns_input.items():
                paths = self._datamanager.getFile(lfns, job_path)
                if paths:
                    new_paths[input_name] = paths
        return new_paths

    def update_inputs(self, inputs: Any, updates: dict[str, Path | list[Path]]):
        """Update CWL job inputs with new file paths.

        This method updates the `inputs.cwl` object by replacing or adding
        file paths for each input specified in `updates`. It supports both
        single files and lists of files.

        Parameters
        ----------
        inputs : JobInputModel
            The job input model whose ``cwl`` dictionary will be updated.
        updates : dict[str, Path | list[Path]]
            Dictionary mapping input names to their corresponding local file
            paths. Each value can be a single `Path` or a list of `Path` objects.

        Notes
        -----
        This method is typically called after downloading LFNs
        using `download_lfns` to ensure that the CWL job inputs reference
        the correct local files.
        """
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
        """Pre-process job inputs and command before execution.

        This method prepares CWL job inputs by performing pre-execution tasks such as:
        - downloading LFNs,
        - updating CWL input definitions with local file paths,
        - and serializing the final input parameters into a YAML file added to the command line.

        The default implementation performs standard preparation steps,
        but this method is designed to be **overridden by subclasses**
        to implement custom pre-processing logic such as:
        - specialized data staging or fetching strategies,
        - environment setup before command execution.

        Parameters
        ----------
        executable : CommandLineTool | Workflow | ExpressionTool
            The CWL tool, workflow, or expression to be executed.
        arguments : JobInputModel, optional
            The job inputs, including CWL and LFN data.
        job_path : Path
            Path to the job working directory.
        command : list[str]
            The command to be executed, which will be modified.
        **kwargs : Any
            Additional parameters, allowing extensions to pass extra context
            or configuration options.

        Returns
        -------
        list[str]
            The modified command, typically including the serialized CWL
            input file path.

        Notes
        -----
        Subclasses may override this method to customize pre-processing behavior.
        When overriding, it is recommended to call ``super().pre_process(...)``
        if the base pre-processing logic should be preserved.
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

    def post_process(
        self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any
    ) -> bool:
        """Post-process job outputs.

        Parameters
        ----------
        job_path : Path
            Path to the job working directory.
        """
        # Get the outputs and outputted files from the cwltool standard output
        # if stdout:
        #     outputs = self.get_job_outputted_files(stdout)
        #     for output, file_paths in outputs.items():
        #         self.store_output(output, file_paths)
        return True

    def get_job_outputted_files(self, stdout: str) -> dict[str, list[str]]:
        """Get the outputted filepaths per output.

        Parameters
        ----------
        stdout : str
            The console output of the the job

        Returns
        ----------
        dict[str, list[str]]
            The dict of the list of filepaths for each output
        """
        outputted_files: dict[str, list[str]] = {}
        outputs = json.loads(stdout)
        for output, files in outputs.items():
            if not files:
                continue
            if not isinstance(files, List):
                files = [files]
            file_paths = []
            for file in files:
                if file:
                    file_paths.append(str(file["path"]))
            outputted_files[output] = file_paths
        return outputted_files

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Delegate to data catalog interface."""
        return None

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Delegate to data catalog interface."""
        return None

    def store_output(
        self,
        output_name: str,
        src_path: str | Path | Sequence[str | Path],
        **kwargs: Any,
    ) -> None:
        """Delegate to the correct interface."""
        logger.info(f"Storing output {output_name}, with source {src_path}")

        # TODO use a hint to differentiate Sandboxes and data outputs
        if output_name == "SandboxOutput":
            if isinstance(src_path, Path) or isinstance(src_path, str):
                src_path = [src_path]
            SandboxStoreClient().uploadFilesAsSandbox(src_path)
        else:
            lfn = self.get_output_query(output_name)
            if lfn:
                self._datamanager.putAndRegister(str(lfn), src_path)

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

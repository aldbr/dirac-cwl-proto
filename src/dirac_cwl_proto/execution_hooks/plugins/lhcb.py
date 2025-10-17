"""LHCb experiment metadata plugins.

This module demonstrates how to implement experiment-specific metadata models
for the LHCb experiment, showcasing the extensibility of the DIRAC metadata system.
"""

from __future__ import annotations

import glob
import random
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union, cast

from cwl_utils.parser import load_document_by_uri, save
from cwl_utils.parser.cwl_v1_2 import Saveable
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from pydantic import Field
from ruamel.yaml import YAML

from ..core import DataCatalogInterface, ExecutionHooksBasePlugin


class LHCbDataCatalogInterface(DataCatalogInterface):
    """Unified data catalog interface for LHCb experiment.

    Handles simulation, reconstruction, and analysis workflows for LHCb.
    """

    def __init__(self):
        """Initialize LHCb data catalog interface."""
        pass

    def get_lhcb_base_path(self, task_id: int, run_id: int) -> Path:
        """Get the base path for LHCb data organization."""
        return Path("filecatalog") / "lhcb" / str(task_id) / str(run_id)

    def get_input_query(  # type: ignore[override]
        self,
        input_name: str,
        task_id: int,
        run_id: int,
        workflow_type: str = "simulation",
        input_data_type: str = "RAW",
        user_name: Optional[str] = None,
        analysis_name: Optional[str] = None,
        input_datasets: Optional[List[str]] = None,
    ) -> Union[Path, List[Path], None]:
        """Get input query for LHCb workflows."""
        # Analysis workflow
        if workflow_type == "analysis":
            # user_name and analysis_name are required for analysis workflows
            if not user_name or not analysis_name:
                raise ValueError(
                    "user_name and analysis_name are required for analysis workflows"
                )

            base = Path("filecatalog") / "lhcb" / "analysis" / user_name / analysis_name

            if input_datasets:
                # Return paths for all specified datasets
                dataset_paths = []
                for dataset in input_datasets:
                    dataset_paths.append(base / "datasets" / dataset)
                return dataset_paths

            return base / "input"

        # Reconstruction workflow
        elif workflow_type == "reconstruction":
            base = self.get_lhcb_base_path(task_id, run_id)

            if input_name == "raw_data":
                result = base / "raw"
            elif input_name == "conditions":
                result = base / "conditions"
            elif input_name == "input_files":
                # Use input_data_type to determine the subdirectory
                result = base / input_data_type.lower()
            elif input_name == "files":
                # Files input typically refers to simulation output files
                result = base / "simulation"
            else:
                result = base

            return result

        # Simulation workflow (default)
        else:
            if input_name == "conditions":
                return self.get_lhcb_base_path(task_id, run_id) / "conditions"
            elif input_name == "geometry":
                return self.get_lhcb_base_path(task_id, run_id) / "geometry"

            return self.get_lhcb_base_path(task_id, run_id)

    def get_output_query(  # type: ignore[override]
        self,
        output_name: str,
        task_id: int,
        run_id: int,
        workflow_type: str = "simulation",
        output_data_type: str = "DST",
        user_name: Optional[str] = None,
        analysis_name: Optional[str] = None,
        analysis_version: Optional[str] = None,
    ) -> Optional[Path]:
        """Get output path for LHCb workflows."""
        # Analysis workflow
        if workflow_type == "analysis":
            # user_name and analysis_name are required for analysis workflows
            if not user_name or not analysis_name:
                raise ValueError(
                    "user_name and analysis_name are required for analysis workflows"
                )

            base = Path("filecatalog") / "lhcb" / "analysis" / user_name / analysis_name

            if analysis_version:
                base = base / analysis_version

            if output_name == "ntuples":
                return base / "ntuples"
            elif output_name == "histograms":
                return base / "histograms"
            elif output_name == "plots":
                return base / "plots"

            return base / "results"

        # Reconstruction workflow
        elif workflow_type == "reconstruction":
            base = self.get_lhcb_base_path(task_id, run_id)

            if output_name == "dst":
                return base / "reconstruction" / output_data_type.lower()
            elif output_name == "log":
                return base / "logs"
            elif output_name == "output_files":
                # Use output_data_type to determine the subdirectory
                return base / output_data_type.lower()

            return base / "outputs"

        # Simulation workflow (default)
        else:
            base = self.get_lhcb_base_path(task_id, run_id)

            if output_name == "sim":
                return base / "simulation"
            elif output_name == "pool_xml_catalog":
                return base / "catalogs"

            return base / "outputs"


class LHCbBasePlugin(ExecutionHooksBasePlugin):
    """Base metadata model for LHCb experiment.

    This class provides common functionality for all LHCb metadata models,
    including standard path structures and experiment-specific validation.
    """

    vo: ClassVar[str] = "lhcb"

    # LHCb-specific fields
    task_id: int = Field(description="LHCb task identifier")
    run_id: int = Field(description="LHCb run identifier")


class LHCbSimulationPlugin(LHCbBasePlugin):
    """Metadata model for LHCb simulation jobs.

    This model handles the specific requirements of LHCb simulation jobs,
    including dynamic event number calculation based on available resources.
    """

    description: ClassVar[
        str
    ] = "LHCb simulation metadata with dynamic resource allocation"

    number_of_events: int = Field(default=0, description="Number of events to simulate")

    # LHCb simulation parameters
    detector_conditions: Optional[str] = Field(
        default=None, description="Detector conditions tag"
    )

    beam_energy: Optional[float] = Field(default=None, description="Beam energy in GeV")

    generator_config: Optional[str] = Field(
        default=None, description="Generator configuration"
    )

    def __init__(self, **kwargs: Any):
        """Initialize with unified LHCb data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = LHCbDataCatalogInterface()

    def pre_process(
        self, job_path: Path, command: List[str], **kwargs: Any
    ) -> List[str]:
        """Pre-process LHCb simulation job.

        This method calculates the optimal number of events to simulate
        based on available CPU resources and job requirements.
        """
        # Load the CWL document to get resource requirements
        if len(command) > 1:
            try:
                task = load_document_by_uri(job_path / command[1])
                cores_min = 1
                cores_max = 1

                # Extract resource requirements
                for requirement in getattr(task, "requirements", []):
                    if requirement.class_ == "ResourceRequirement":
                        cores_min = getattr(requirement, "coresMin", 1) or 1
                        cores_max = getattr(requirement, "coresMax", 1) or 1

                # Simulate worker node capabilities
                available_processors = random.randint(1, 8)

                if available_processors < cores_min:
                    raise RuntimeError(
                        f"Insufficient processors: need {cores_min}, have {available_processors}"
                    )

                processors_to_use = min(available_processors, cores_max)

                # Calculate optimal event count based on simulated performance
                cpu_power_factor = random.uniform(0.8, 1.2)  # CPU variation
                base_time_per_event = 10.0  # seconds per event (base)
                max_job_time = 3600 * 8  # 8 hours max

                events_per_core = int(
                    (max_job_time * cpu_power_factor) / base_time_per_event
                )

                self.number_of_events = events_per_core * processors_to_use

                # Update job parameters
                self._update_job_parameters(job_path, command)

            except Exception as e:
                # Fallback to default if processing fails
                self.number_of_events = 1000
                print(
                    f"Warning: Failed to calculate optimal events, using default: {e}"
                )

        return command

    def _update_job_parameters(self, job_path: Path, command: List[str]) -> None:
        """Update job parameters with calculated values."""
        if len(command) >= 3:
            parameters_path = job_path / command[2]
            try:
                parameters = load_inputfile(str(parameters_path))
            except Exception:
                parameters = {}
        else:
            parameters_path = job_path / "lhcb_parameters.yaml"
            parameters = {}
            command.append(parameters_path.name)

        # Update parameters with LHCb-specific values
        parameters.update(
            {
                "number-of-events": self.number_of_events,
                "task-id": self.task_id,
                "run-id": self.run_id,
            }
        )

        if self.detector_conditions:
            parameters["detector-conditions"] = self.detector_conditions
        if self.beam_energy:
            parameters["beam-energy"] = self.beam_energy
        if self.generator_config:
            parameters["generator-config"] = self.generator_config

        # Save parameters
        parameter_dict = save(cast(Saveable, parameters))
        with open(parameters_path, "w") as f:
            YAML().dump(parameter_dict, f)

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Get input query for simulation workflow."""
        return self.data_catalog.get_input_query(
            input_name,
            task_id=self.task_id,
            run_id=self.run_id,
            workflow_type="simulation",
            **kwargs,
        )

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Get output query for simulation workflow."""
        return self.data_catalog.get_output_query(
            output_name,
            task_id=self.task_id,
            run_id=self.run_id,
            workflow_type="simulation",
            **kwargs,
        )

    def post_process(
        self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any
    ) -> bool:
        """Post-process LHCb simulation outputs."""
        success = True

        # Store simulation files
        sim_files = glob.glob(str(job_path / "*.sim"))
        for sim_file in sim_files:
            try:
                self.store_output(
                    "sim", sim_file, task_id=self.task_id, run_id=self.run_id
                )
            except Exception as e:
                print(f"Failed to store simulation output {sim_file}: {e}")
                success = False

        # Store catalog files
        catalog_files = glob.glob(str(job_path / "pool_xml_catalog.xml"))
        for catalog_file in catalog_files:
            try:
                self.store_output(
                    "pool_xml_catalog",
                    catalog_file,
                    task_id=self.task_id,
                    run_id=self.run_id,
                )
            except Exception as e:
                print(f"Failed to store catalog {catalog_file}: {e}")
                success = False

        return success


class LHCbReconstructionPlugin(LHCbBasePlugin):
    """Metadata model for LHCb reconstruction jobs.

    This model handles LHCb data reconstruction, including input file
    management and output organization.
    """

    description: ClassVar[str] = "LHCb reconstruction metadata with file management"

    # Reconstruction-specific parameters
    reconstruction_version: Optional[str] = Field(
        default=None, description="Reconstruction software version"
    )

    input_data_type: str = Field(
        default="RAW", description="Type of input data (RAW, DST, etc.)"
    )

    output_data_type: str = Field(default="DST", description="Type of output data")

    def __init__(self, **kwargs: Any):
        """Initialize with unified LHCb data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = LHCbDataCatalogInterface()

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Get input query for reconstruction workflow."""
        return self.data_catalog.get_input_query(
            input_name,
            task_id=self.task_id,
            run_id=self.run_id,
            workflow_type="reconstruction",
            input_data_type=self.input_data_type,
            **kwargs,
        )

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Get output query for reconstruction workflow."""
        return self.data_catalog.get_output_query(
            output_name,
            task_id=self.task_id,
            run_id=self.run_id,
            workflow_type="reconstruction",
            output_data_type=self.output_data_type,
            **kwargs,
        )

    def pre_process(
        self, job_path: Path, command: List[str], **kwargs: Any
    ) -> List[str]:
        """Pre-process the job command for reconstruction."""
        # Only add LHCb-specific arguments if this is not a CWL workflow execution
        # (i.e., when running LHCb applications directly, not cwltool)
        if not command or command[0] != "cwltool":
            # Add reconstruction version if specified
            if self.reconstruction_version:
                command.extend(["--version", self.reconstruction_version])

            # Add data type parameters
            command.extend(["--input-type", self.input_data_type])
            command.extend(["--output-type", self.output_data_type])

        return command

    def post_process(
        self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any
    ) -> bool:
        """Post-process LHCb reconstruction outputs."""
        success = True

        # Store reconstructed data files
        dst_files = glob.glob(str(job_path / "*.dst"))
        for dst_file in dst_files:
            try:
                self.store_output(
                    "dst", dst_file, task_id=self.task_id, run_id=self.run_id
                )
            except Exception as e:
                print(f"Failed to store DST output {dst_file}: {e}")
                success = False

        # Store log files
        log_files = glob.glob(str(job_path / "*.log"))
        for log_file in log_files:
            try:
                self.store_output(
                    "log", log_file, task_id=self.task_id, run_id=self.run_id
                )
            except Exception as e:
                print(f"Failed to store log {log_file}: {e}")
                success = False

        return success


class LHCbAnalysisPlugin(LHCbBasePlugin):
    """Metadata model for LHCb analysis jobs.

    This model supports user analysis jobs with flexible input/output
    handling and analysis-specific configurations.
    """

    description: ClassVar[str] = "LHCb analysis metadata for user analysis jobs"

    # Analysis-specific parameters
    analysis_name: str = Field(description="Name of the analysis")
    analysis_version: Optional[str] = Field(
        default=None, description="Analysis version"
    )
    user_name: str = Field(description="Analysis owner")

    input_datasets: Optional[List[str]] = Field(
        default=None, description="List of input dataset identifiers"
    )

    selection_criteria: Optional[str] = Field(
        default=None, description="Event selection criteria"
    )

    def __init__(self, **kwargs: Any):
        """Initialize with unified LHCb data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = LHCbDataCatalogInterface()

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Get input query for analysis workflow."""
        return self.data_catalog.get_input_query(
            input_name,
            task_id=self.task_id,
            run_id=self.run_id,
            workflow_type="analysis",
            user_name=self.user_name,
            analysis_name=self.analysis_name,
            input_datasets=self.input_datasets,
            **kwargs,
        )

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Get output query for analysis workflow."""
        return self.data_catalog.get_output_query(
            output_name,
            task_id=self.task_id,
            run_id=self.run_id,
            workflow_type="analysis",
            user_name=self.user_name,
            analysis_name=self.analysis_name,
            analysis_version=self.analysis_version,
            **kwargs,
        )

    def pre_process(
        self, job_path: Path, command: List[str], **kwargs: Any
    ) -> List[str]:
        """Pre-process LHCb analysis job."""
        # Add analysis-specific parameters
        command.extend(["--analysis", self.analysis_name])
        command.extend(["--user", self.user_name])

        if self.analysis_version:
            command.extend(["--version", self.analysis_version])

        if self.selection_criteria:
            command.extend(["--selection", self.selection_criteria])

        return command

    def post_process(
        self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any
    ) -> bool:
        """Post-process LHCb analysis outputs."""
        success = True

        # Store ROOT files (ntuples, histograms)
        root_files = glob.glob(str(job_path / "*.root"))
        for root_file in root_files:
            try:
                if "ntuple" in Path(root_file).name.lower():
                    self.store_output("ntuples", root_file)
                elif "hist" in Path(root_file).name.lower():
                    self.store_output("histograms", root_file)
                else:
                    self.store_output("ntuples", root_file)  # Default to ntuples
            except Exception as e:
                print(f"Failed to store ROOT file {root_file}: {e}")
                success = False

        # Store plot files
        plot_extensions = ["*.png", "*.pdf", "*.eps", "*.svg"]
        for ext in plot_extensions:
            plot_files = glob.glob(str(job_path / ext))
            for plot_file in plot_files:
                try:
                    self.store_output("plots", plot_file)
                except Exception as e:
                    print(f"Failed to store plot {plot_file}: {e}")
                    success = False

        return success

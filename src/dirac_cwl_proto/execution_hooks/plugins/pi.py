"""PI simulation metadata models.

This module contains metadata models for PI simulation workflows,
including simulation, gathering, and result processing.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from ..core import DataCatalogInterface, ExecutionHooksBasePlugin


class PiDataCatalogInterface(DataCatalogInterface):
    """Unified data catalog interface for PI workflows.

    Handles simulation, simulation V2, and gathering workflows for PI calculations.
    """

    def __init__(
        self,
        num_points: int,
        # PI Gather parameters
        input_data: Optional[List] = None,
        # PI Simulate V2 parameters
        output_path: Optional[str] = None,
        # Force workflow type detection
        is_gather_workflow: bool = False,
        is_simulate_v2_workflow: bool = False,
    ):
        """Initialize with PI workflow-specific parameters.

        Parameters
        ----------
        num_points : int
            Number of points to simulate or gather
        input_data : List, optional
            List of input data files for gathering workflow
        output_path : str, optional
            Custom output path for simulation V2 workflow
        is_gather_workflow : bool, optional
            Force detection as gather workflow
        is_simulate_v2_workflow : bool, optional
            Force detection as simulate V2 workflow
        """
        self.num_points = num_points
        self.input_data = input_data
        self.output_path = output_path
        self._force_gather_workflow = is_gather_workflow
        self._force_simulate_v2_workflow = is_simulate_v2_workflow

    def _is_gather_workflow(self) -> bool:
        """Check if this is a gathering workflow."""
        return self._force_gather_workflow or self.input_data is not None

    def _is_simulate_v2_workflow(self) -> bool:
        """Check if this is a simulation V2 workflow."""
        return self._force_simulate_v2_workflow or self.output_path is not None

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Get input query for PI workflows."""
        # Gather workflow - references simulation outputs
        if self._is_gather_workflow():
            if input_name == "input-data":
                # Reference the output from PiSimulate
                return Path("filecatalog") / "pi" / str(self.num_points)
            return None

        # Simulation workflows (V1 and V2) - no input queries
        return None

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Get output path for PI workflows."""
        # Gather workflow
        if self._is_gather_workflow():
            if output_name == "pi_result" and self.input_data:
                total_points = self.num_points * len(self.input_data)
                return Path("filecatalog") / "pi" / str(total_points)
            return None

        # Simulation workflows (V1 and V2)
        else:
            if output_name == "sim":
                return Path("filecatalog") / "pi" / str(self.num_points)
            return None


class PiSimulatePlugin(ExecutionHooksBasePlugin):
    """PI simulation metadata model.

    This model handles PI simulation jobs that generate simulation data
    based on the number of points specified.

    Parameters
    ----------
    num_points : int
        Number of points to simulate.
    """

    description: ClassVar[
        str
    ] = "PI simulation metadata with point-based output management"

    num_points: int

    def __init__(self, **kwargs: Any):
        """Initialize with unified PI data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = PiDataCatalogInterface(self.num_points)

    def post_process(self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any) -> bool:
        """Post process the simulation outputs."""
        outputs = glob.glob(str(job_path / "*.sim"))
        if outputs:
            self.store_output("sim", outputs[0])
            return True
        return False


class PiSimulateV2Plugin(ExecutionHooksBasePlugin):
    """PI simulation metadata model version 2.

    Enhanced version with configurable output path.

    Parameters
    ----------
    num_points : int
        Number of points to simulate.
    output_path : str
        Custom output path for simulation results.
    """

    description: ClassVar[
        str
    ] = "Enhanced PI simulation metadata with custom output paths"

    num_points: int
    output_path: str

    def __init__(self, **kwargs: Any):
        """Initialize with unified PI data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = PiDataCatalogInterface(
            self.num_points,
            output_path=self.output_path,
            is_simulate_v2_workflow=True,
        )

    def post_process(self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any) -> bool:
        """Post process the simulation outputs."""
        outputs = job_path / self.output_path
        if outputs.exists():
            self.store_output("sim", str(outputs))
            return True
        return False


class PiGatherPlugin(ExecutionHooksBasePlugin):
    """PI gathering metadata model.

    This model handles gathering and aggregation of PI simulation results
    from multiple simulation jobs.

    Parameters
    ----------
    num_points : int
        Number of points per simulation job.
    input_data : List, optional
        List of input data files to process.
    """

    description: ClassVar[str] = "PI result gathering and aggregation metadata"

    # Query parameters
    num_points: int
    # Input data
    input_data: Optional[List] = None

    def __init__(self, **kwargs: Any):
        """Initialize with unified PI data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = PiDataCatalogInterface(
            self.num_points, input_data=self.input_data, is_gather_workflow=True
        )

    def post_process(self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any) -> bool:
        """Post process the gathered results."""
        outputs = glob.glob(str(job_path / "*.sim"))
        if outputs:
            self.store_output("pi_result", outputs[0])
            return True
        return False

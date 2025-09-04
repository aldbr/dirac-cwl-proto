"""PI simulation metadata models.

This module contains metadata models for PI simulation workflows,
including simulation, gathering, and result processing.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from ..core import ExecutionHooksBasePlugin


class PiSimulateMetadata(ExecutionHooksBasePlugin):
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

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Get output path for simulation results."""
        if output_name == "sim":
            return Path("filecatalog") / "pi" / str(self.num_points)
        return None

    def post_process(self, job_path: Path, **kwargs: Any) -> bool:
        """Post process the simulation outputs."""
        outputs = glob.glob(str(job_path / "*.sim"))
        if outputs:
            self.store_output("sim", outputs[0])
            return True
        return False


class PiSimulateV2Metadata(ExecutionHooksBasePlugin):
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

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Get output path for simulation results."""
        if output_name == "sim":
            return Path("filecatalog") / "pi" / str(self.num_points)
        return None

    def post_process(self, job_path: Path, **kwargs: Any) -> bool:
        """Post process the simulation outputs."""
        outputs = job_path / self.output_path
        if outputs.exists():
            self.store_output("sim", str(outputs))
            return True
        return False


class PiGatherMetadata(ExecutionHooksBasePlugin):
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

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Get input query for simulation data."""
        if input_name == "input-data":
            # Reference the output from PiSimulate
            return Path("filecatalog") / "pi" / str(self.num_points)
        return None

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Get output path for gathered results."""
        if output_name == "pi_result" and self.input_data:
            total_points = self.num_points * len(self.input_data)
            return Path("filecatalog") / "pi" / str(total_points)
        return None

    def post_process(self, job_path: Path, **kwargs: Any) -> bool:
        """Post process the gathered results."""
        outputs = glob.glob(str(job_path / "*.sim"))
        if outputs:
            self.store_output("pi_result", outputs[0])
            return True
        return False

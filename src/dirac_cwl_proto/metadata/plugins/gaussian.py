"""Gaussian fit data analysis metadata models.

This module contains metadata models for Gaussian fitting workflows,
including data generation and fitting analysis.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from ..core import BaseMetadataModel


class DataGenerationMetadata(BaseMetadataModel):
    """Data generation metadata model for Gaussian fitting.

    This model handles generation of test data for Gaussian fitting algorithms.
    It supports generating multiple output files with configurable names.

    Parameters
    ----------
    output_file_name_1 : str, optional
        Name of the first output file.
    output_file_name_2 : str, optional
        Name of the second output file.
    """

    description: ClassVar[str] = "Data generation for Gaussian fitting workflows"

    output_file_name_1: Optional[str] = None
    output_file_name_2: Optional[str] = None

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Get output path for generated data files."""
        base_path = Path("filecatalog") / "gaussian_fit"

        if self.output_file_name_1 and output_name == "data1":
            return base_path / "data-generation-1"
        if self.output_file_name_2 and output_name == "data2":
            return base_path / "data-generation-2"

        return base_path / "data-generation"

    def post_process(self, job_path: Path) -> bool:
        """Post process the generated data files."""
        success = False

        # Process first output file
        if self.output_file_name_1:
            outputs = glob.glob(str(job_path / self.output_file_name_1))
            if outputs:
                self.store_output("data1", outputs[0])
                success = True

        # Process second output file
        if self.output_file_name_2:
            outputs = glob.glob(str(job_path / self.output_file_name_2))
            if outputs:
                self.store_output("data2", outputs[0])
                success = True

        # If no specific files, process all
        if not self.output_file_name_1 and not self.output_file_name_2:
            outputs = glob.glob(str(job_path / "*"))
            if outputs:
                self.store_output("data1", outputs[0])
                success = True

        return success


class GaussianFitMetadata(BaseMetadataModel):
    """Gaussian fitting metadata model.

    This model handles Gaussian fitting analysis on generated data sets.
    It processes multiple input data files and produces fitting results.

    Parameters
    ----------
    data1 : List, optional
        First set of input data files.
    data2 : List, optional
        Second set of input data files.
    """

    description: ClassVar[str] = "Gaussian fitting analysis on data sets"

    # Input data
    data1: Optional[List] = None
    data2: Optional[List] = None

    def get_input_query(self, input_name: str, **kwargs: Any) -> Union[Path, List[Path], None]:
        """Get input query for data files to fit."""
        base_path = Path("filecatalog") / "gaussian_fit"

        if input_name == "data1":
            return base_path / "data-generation-1"
        if input_name == "data2":
            return base_path / "data-generation-2"

        return None

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Get output path for fitting results."""
        if output_name == "fit-data" and (self.data1 or self.data2):
            return Path("filecatalog") / "gaussian_fit" / "fit"
        return None

    def post_process(self, job_path: Path) -> bool:
        """Post process the fitting results."""
        outputs = glob.glob(str(job_path / "fit*"))
        if outputs:
            self.store_output("fit-data", outputs[0])
            return True
        return False

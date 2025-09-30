"""Gaussian fit data analysis metadata models.

This module contains metadata models for Gaussian fitting workflows,
including data generation and fitting analysis.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from ..core import DataCatalogInterface, ExecutionHooksBasePlugin


class GaussianDataCatalogInterface(DataCatalogInterface):
    """Unified data catalog interface for Gaussian workflows.

    Handles both data generation and fitting scenarios for Gaussian workflows.
    """

    def __init__(
        self,
        output_file_name_1: Optional[str] = None,
        output_file_name_2: Optional[str] = None,
        data1: Optional[List] = None,
        data2: Optional[List] = None,
    ):
        """Initialize with data generation and fitting parameters.

        Parameters
        ----------
        output_file_name_1 : str, optional
            Name of the first output file for data generation.
        output_file_name_2 : str, optional
            Name of the second output file for data generation.
        data1 : List, optional
            First set of input data files for fitting.
        data2 : List, optional
            Second set of input data files for fitting.
        """
        self.output_file_name_1 = output_file_name_1
        self.output_file_name_2 = output_file_name_2
        self.data1 = data1
        self.data2 = data2

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Get input query for data files.

        Returns None for data generation, specific paths for fitting.
        """
        # For data generation workflows, no input queries
        if not self.data1 and not self.data2:
            return None

        # For fitting workflows, provide input data paths
        base_path = Path("filecatalog") / "gaussian_fit"

        if input_name == "data1":
            return base_path / "data-generation-1"
        if input_name == "data2":
            return base_path / "data-generation-2"

        return None

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Get output path for both generation and fitting."""
        base_path = Path("filecatalog") / "gaussian_fit"

        # Data generation outputs
        if self.output_file_name_1 and output_name == "data1":
            return base_path / "data-generation-1"
        if self.output_file_name_2 and output_name == "data2":
            return base_path / "data-generation-2"

        # Default data generation output
        if output_name in ("data1", "data2") and not (self.data1 or self.data2):
            return base_path / "data-generation"

        # Fitting output
        if output_name == "fit-data" and (self.data1 or self.data2):
            return base_path / "fit"

        return None


class DataGenerationPlugin(ExecutionHooksBasePlugin):
    """Data generation metadata model for Gaussian fitting.

    This plugin handles generation of test data for Gaussian fitting algorithms.:w

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

    def __init__(self, **kwargs: Any):
        """Initialize with unified Gaussian data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = GaussianDataCatalogInterface(
            output_file_name_1=self.output_file_name_1,
            output_file_name_2=self.output_file_name_2,
        )

    def post_process(
        self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any
    ) -> bool:
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


class GaussianFitPlugin(ExecutionHooksBasePlugin):
    """Gaussian fitting metadata model.

    This plugin handles Gaussian fitting analysis on generated data sets.
    It processes multiple input data files and produces fitting results.

    Parameters
    ----------
    data1 : List, optional
        First set of input data files.
    data2 : List, optional
        Second set of input data files.
    """

    description: ClassVar[str] = "Gaussian fitting analysis for generated data"

    # Input data
    data1: Optional[List] = None
    data2: Optional[List] = None

    def __init__(self, **kwargs: Any):
        """Initialize with unified Gaussian data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = GaussianDataCatalogInterface(
            data1=self.data1, data2=self.data2
        )

    def post_process(
        self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any
    ) -> bool:
        """Post process the fitting results."""
        outputs = glob.glob(str(job_path / "fit*"))
        if outputs:
            self.store_output("fit-data", outputs[0])
            return True
        return False

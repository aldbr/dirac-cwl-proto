"""Mandelbrot set generation metadata models.

This module contains metadata models for Mandelbrot set generation and
image processing workflows.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from ..core import DataCatalogInterface, ExecutionHooksBasePlugin


class MandelbrotDataCatalogInterface(DataCatalogInterface):
    """Unified data catalog interface for Mandelbrot workflows.

    Handles both generation and merging workflows for Mandelbrot set calculations.
    """

    def __init__(
        self,
        width: int,
        height: int,
        # Merging parameters
        data: Optional[List] = None,
        # Force workflow detection flags
        is_merging_workflow: bool = False,
    ):
        """Initialize with Mandelbrot workflow-specific parameters.

        Parameters
        ----------
        width : int
            Image width in pixels
        height : int
            Image height in pixels
        data : List, optional
            List of input data files for merging workflow
        is_merging_workflow : bool, optional
            Force detection as merging workflow regardless of data state
        """
        self.width = width
        self.height = height
        self.data = data
        self._force_merging_workflow = is_merging_workflow

    def _is_merging_workflow(self) -> bool:
        """Check if this is a merging workflow."""
        return self._force_merging_workflow or (self.data is not None)

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Get input query for Mandelbrot workflows."""
        # Merging workflow - references generation outputs
        if self._is_merging_workflow():
            # Reference the output from MandelBrotGeneration
            return (
                Path("filecatalog")
                / "mandelbrot"
                / "images"
                / "raw"
                / f"{self.width}x{self.height}"
            )

        # Generation workflow - no input queries
        return None

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Get output path for Mandelbrot workflows."""
        # Merging workflow
        if self._is_merging_workflow():
            if output_name == "data-merged" and self.data:
                width = len(self.data) * self.width
                height = len(self.data) * self.height
                return (
                    Path("filecatalog")
                    / "mandelbrot"
                    / "images"
                    / "merged"
                    / f"{width}x{height}"
                )
            return None

        # Generation workflow
        else:
            if output_name == "data":
                return (
                    Path("filecatalog")
                    / "mandelbrot"
                    / "images"
                    / "raw"
                    / f"{self.width}x{self.height}"
                )
            return None


class MandelBrotGenerationPlugin(ExecutionHooksBasePlugin):
    """Mandelbrot set generation metadata model.

    This model handles the generation of Mandelbrot set data with specified
    parameters for precision, iterations, and image dimensions.

    Parameters
    ----------
    precision : float
        Numerical precision for calculations.
    max_iterations : int
        Maximum number of iterations for convergence.
    start_x : float
        Starting X coordinate.
    start_y : float
        Starting Y coordinate.
    step : int
        Step size for calculations.
    split : int
        Number of splits for parallel processing.
    width : int
        Image width in pixels.
    height : int
        Image height in pixels.
    output_name : str
        Name for output files.
    """

    description: ClassVar[
        str
    ] = "Mandelbrot set generation with configurable parameters"

    precision: float
    max_iterations: int
    start_x: float
    start_y: float
    step: int
    split: int
    width: int
    height: int
    output_name: str

    def __init__(self, **kwargs: Any):
        """Initialize with unified Mandelbrot data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = MandelbrotDataCatalogInterface(self.width, self.height)

    def post_process(self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any) -> bool:
        """Post process the generated data files."""
        outputs = glob.glob(str(job_path / "data*.txt"))
        if outputs:
            self.store_output("data", outputs[0])
            return True
        return False


class MandelBrotMergingPlugin(ExecutionHooksBasePlugin):
    """Mandelbrot set merging metadata model.

    This model handles merging of multiple Mandelbrot data files into
    composite images.

    Parameters
    ----------
    precision : float
        Numerical precision for calculations.
    max_iterations : int
        Maximum number of iterations for convergence.
    start_x : float
        Starting X coordinate.
    start_y : float
        Starting Y coordinate.
    step : int
        Step size for calculations.
    split : int
        Number of splits for parallel processing.
    width : int
        Image width in pixels.
    height : int
        Image height in pixels.
    output_name : str
        Name for output files.
    data : List, optional
        List of input data files to merge.
    """

    description: ClassVar[str] = "Mandelbrot image merging and composite generation"

    # Query parameters
    precision: float
    max_iterations: int
    start_x: float
    start_y: float
    step: int
    split: int
    width: int
    height: int
    output_name: str
    # Input data
    data: Optional[List] = None

    def __init__(self, **kwargs: Any):
        """Initialize with unified Mandelbrot data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = MandelbrotDataCatalogInterface(
            self.width, self.height, data=self.data, is_merging_workflow=True
        )

    def post_process(self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any) -> bool:
        """Post process the merged image files."""
        outputs = glob.glob(str(job_path / "mandelbrot_image*bmp"))
        if outputs:
            self.store_output("data-merged", outputs[0])
            return True
        return False

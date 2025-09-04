"""Mandelbrot set generation metadata models.

This module contains metadata models for Mandelbrot set generation and
image processing workflows.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from ..core import ExecutionHooksBasePlugin


class MandelBrotGenerationMetadata(ExecutionHooksBasePlugin):
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

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Get output path for generated Mandelbrot data."""
        if output_name == "data":
            return (
                Path("filecatalog")
                / "mandelbrot"
                / "images"
                / "raw"
                / f"{self.width}x{self.height}"
            )
        return None

    def post_process(self, job_path: Path, **kwargs: Any) -> bool:
        """Post process the generated data files."""
        outputs = glob.glob(str(job_path / "data*.txt"))
        if outputs:
            self.store_output("data", outputs[0])
            return True
        return False


class MandelBrotMergingMetadata(ExecutionHooksBasePlugin):
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

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Get input query for Mandelbrot data to merge."""
        # Reference the output from MandelBrotGeneration
        return (
            Path("filecatalog")
            / "mandelbrot"
            / "images"
            / "raw"
            / f"{self.width}x{self.height}"
        )

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Get output path for merged Mandelbrot images."""
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

    def post_process(self, job_path: Path, **kwargs: Any) -> bool:
        """Post process the merged image files."""
        outputs = glob.glob(str(job_path / "mandelbrot_image*bmp"))
        if outputs:
            self.store_output("data-merged", outputs[0])
            return True
        return False

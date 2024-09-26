import glob
import logging
import os
from pathlib import Path
from typing import List

from pydantic import BaseModel

# -----------------------------------------------------------------------------
# Metadata models (Job Type)
# -----------------------------------------------------------------------------


class IMetadataModel(BaseModel):
    """Metadata for a transformation."""

    def get_input_query(self, input_name: str) -> Path | None:
        """
        Template method for getting the input path where the inputs of a job are stored.
        Should be overridden by subclasses.
        """
        return None

    def get_output_query(self, output_name: str) -> Path | None:
        """
        Template method for getting the output path to store results of a job.
        Should be overridden by subclasses.
        """
        return None

    def pre_process(self, command: List[str]) -> List[str]:
        """
        Template method for process the inputs of a job.
        Should be overriden by subclasses.
        """
        return command

    def post_process(self):
        """
        Template method for processing the outputs of a job.
        Should be overridden by subclasses.
        """
        pass

    def _store_output(self, output_name: str, output_value: str):
        """Store the output in the "filecatalog" directory."""
        # Get the output query
        output_path = self.get_output_query(output_name)
        if not output_path:
            raise RuntimeError("No output path defined.")
        output_path.mkdir(exist_ok=True, parents=True)

        # Send the output to the file catalog
        output = output_path / f"{output_value}"
        os.rename(output_value, output)
        logging.info(f"Output stored in {output}")


# -----------------------------------------------------------------------------


class User(IMetadataModel):
    """User metadata model: does nothing."""


# -----------------------------------------------------------------------------


class PiSimulate(IMetadataModel):
    """Pi simulation metadata model."""

    num_points: int

    def get_output_query(self, output_name: str) -> Path | None:
        if output_name == "sim":
            return Path("filecatalog") / "pi" / str(self.num_points)
        return None

    def post_process(self):
        """Post process the outputs of a job."""
        outputs = glob.glob("*.sim")
        if outputs:
            self._store_output("sim", outputs[0])


class PiGather(IMetadataModel):
    """Pi gathering metadata model."""

    # Query parameters
    num_points: int
    # Input data
    input_data: List | None

    def get_input_query(self, input_name: str) -> Path | None:
        if input_name == "input-data":
            return PiSimulate(num_points=self.num_points).get_output_query("sim")
        return None

    def get_output_query(self, output_name: str) -> Path | None:
        if output_name == "pi_result" and self.input_data:
            return (
                Path("filecatalog") / "pi" / str(self.num_points * len(self.input_data))
            )
        return None

    def post_process(self):
        """Post process the outputs of a job."""
        outputs = glob.glob("*sim")
        if outputs:
            self._store_output("pi_result", outputs[0])


# -----------------------------------------------------------------------------


class LHCb(IMetadataModel):
    """LHCb metadata model version 1."""

    task_id: int
    run_id: int

    def get_input_query(self, input_name: str) -> Path | None:
        return Path("filecatalog") / str(self.task_id) / str(self.run_id)

    def get_output_query(self, output_name: str) -> Path | None:
        return Path("filecatalog") / str(self.task_id) / str(self.run_id)

    def post_process(self):
        """Post process the outputs of a job."""
        outputs = glob.glob("*.sim")
        if outputs:
            self._store_output("sim", outputs[0])
        outputs = glob.glob("pool_xml_catalog.xml")
        if outputs:
            self._store_output("pool_xml_catalog", outputs[0])


# -----------------------------------------------------------------------------


class MandelBrotGeneration(IMetadataModel):
    """Mandelbrot Generation metadata model."""

    precision: float
    max_iterations: int
    start_x: float
    start_y: float
    step: int
    split: int
    width: int
    height: int
    output_name: str

    def get_output_query(self, output_name: str) -> Path | None:
        if output_name == "data":
            return (
                Path("filecatalog")
                / "mandelbrot"
                / "images"
                / "raw"
                / f"{self.width}x{self.height}"
            )
        return None

    def post_process(self):
        """Post process the outputs of a job."""
        outputs = glob.glob("data*txt")
        if outputs:
            self._store_output("data", outputs[0])


class MandelBrotMerging(IMetadataModel):
    """Mandelbrot Merging metadata model."""

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
    data: List | None

    def get_input_query(self, input_name: str) -> Path | None:
        return MandelBrotGeneration(
            precision=self.precision,
            max_iterations=self.max_iterations,
            start_x=self.start_x,
            start_y=self.start_y,
            step=self.step,
            split=self.split,
            width=self.width,
            height=self.height,
            output_name=self.output_name,
        ).get_output_query("data")

    def get_output_query(self, output_name: str) -> Path | None:
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

    def post_process(self):
        """Post process the outputs of a job."""
        outputs = glob.glob("mandelbrot_image*bmp")
        if outputs:
            self._store_output("data-merged", outputs[0])

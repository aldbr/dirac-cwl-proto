import glob
import logging
import os
from pathlib import Path

from pydantic import BaseModel


class IMetadataModel(BaseModel):
    """Metadata for a transformation."""

    def get_bk_path(self, output_name: str) -> Path:
        """
        Template method for getting the output path to store results of a job/get results of a previous job.
        Should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def post_process(self) -> bool:
        """
        Template method for processing the outputs of a job.
        Should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def _store_output(self, output_name: str, output_value: str):
        """Store the output in the "bookkeeping" directory."""
        # Create the "bookkeeping" path
        output_path = self.get_bk_path(output_name)

        # Send the output to the "bookkeeping"
        bk_output = output_path / f"{output_value}"
        os.rename(output_value, bk_output)
        logging.info(f"Output stored in {bk_output}")


class BasicMetadataModel(IMetadataModel):
    """Very basic metadata model."""

    max_random: int
    min_random: int

    def get_bk_path(self, output_name: str) -> Path:
        """Get the bk path."""
        output_path = Path("bookkeeping") / str(self.max_random) / str(self.min_random)
        output_path.mkdir(exist_ok=True, parents=True)
        return output_path

    def post_process(self) -> bool:
        """Post process the outputs of a job."""
        outputs = glob.glob("*.sim")
        if outputs:
            self._store_output("sim", outputs[0])
        outputs = glob.glob("output.dst")
        if outputs:
            self._store_output("result", outputs[0])
        return True


class LHCbMetadataModel(IMetadataModel):
    """LHCb metadata model version 1."""

    task_id: int
    run_id: int

    def get_bk_path(self, output_name: str) -> Path:
        """Get the bk path."""
        # Create the "bookkeeping" path
        output_path = Path("bookkeeping") / str(self.task_id) / str(self.run_id)
        output_path.mkdir(exist_ok=True, parents=True)
        return output_path

    def post_process(self) -> bool:
        """Post process the outputs of a job."""
        outputs = glob.glob("*.sim")
        if outputs:
            self._store_output("sim", outputs[0])
        outputs = glob.glob("pool_xml_catalog.xml")
        if outputs:
            self._store_output("pool_xml_catalog", outputs[0])
        return True


class MacobacMetadataModel(IMetadataModel):
    """Very basic metadata model."""

    configuration: Path

    def get_bk_path(self, output_name: str) -> Path:
        """Get the bk path."""
        output_path = Path("bookkeeping") / "macobac"
        output_path.mkdir(exist_ok=True, parents=True)
        return output_path

    def post_process(self) -> bool:
        """Post process the outputs of a job."""
        return True


class MandelBrotMetadataModel(IMetadataModel):
    """Very basic metadata model."""

    precision: float
    max_iterations: int
    start_x: float
    start_y: float
    step: int
    split: int
    width: int
    height: int
    output_name: str

    def get_bk_path(self, output_name: str) -> Path:
        """Get the bk path."""
        if output_name == "data":
            output_path = Path("bookkeeping") / "mandelbrot" / "images" / "raw"
        elif output_name == "data-merged":
            output_path = Path("bookkeeping") / "mandelbrot" / "images" / "merged"
        output_path.mkdir(exist_ok=True, parents=True)
        return output_path

    def post_process(self) -> bool:
        """Post process the outputs of a job."""
        outputs = glob.glob("data_merged*txt")
        if outputs:
            self._store_output("data-merged", outputs[0])

        outputs = glob.glob("data*txt")
        if outputs:
            self._store_output("data", outputs[0])
        return True

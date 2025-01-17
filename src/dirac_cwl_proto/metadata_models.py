import glob
import logging
import math
import os
import random
from pathlib import Path
from typing import List, cast

from cwl_utils.parser import load_document_by_uri, save
from cwl_utils.parser.cwl_v1_2 import Saveable
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from pydantic import BaseModel
from ruamel.yaml import YAML

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

    def pre_process(self, job_path: Path, command: List[str]) -> List[str]:
        """
        Template method for process the inputs of a job.
        Should be overriden by subclasses.
        """
        return command

    def post_process(self, job_path: Path):
        """
        Template method for processing the outputs of a job.
        Should be overridden by subclasses.
        """
        pass

    def _store_output(self, output_name: str, src: str):
        """Store the output in the "filecatalog" directory."""
        # Get the output query
        output_path = self.get_output_query(output_name)
        if not output_path:
            raise RuntimeError("No output path defined.")
        output_path.mkdir(exist_ok=True, parents=True)

        # Send the output to the file catalog
        output_value = Path(src).name
        dest = output_path / output_value
        os.rename(src, dest)
        logging.info(f"Output stored in {dest}")


# -----------------------------------------------------------------------------


class TaskWithMetadataQuery(IMetadataModel):
    """
    TaskWithMetadataQuery is a class providing methods to query metadata and generate input paths based on the metadata.

    Methods
    -------
    get_input_query(**kwargs) -> Path | list[Path] | None
        Generates a query to retrieve input paths based on provided metadata.

    Example
    -------
    >>> query = TaskWithMetadataQuery()
    >>> input_path = query.get_input_query(site="LaPalma", campaign="PROD6")
    >>> print(input_path)
    [PosixPath('filecatalog/PROD6/LaPalma')]

    Attributes
    ----------
    Inherits attributes from IMetadataModel.
    """

    def get_input_query(self, **kwargs) -> Path | list[Path] | None:
        """
        Generates a query to retrieve input paths based on provided metadata.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments representing metadata attributes. Expected keys are:
            - site (str): The site name.
            - campaign (str): The campaign name.

        Returns
        -------
        Path | list[Path] | None
            A Path or list of Paths representing the input query based on the provided metadata.
            Returns None if neither site nor campaign is provided.

        Notes
        -----
        This is an example implementation. In a real implementation,
        an actual query should be made to the metadata service,
        resulting in an array of Logical File Names (LFNs) being returned.
        """
        site = kwargs.get("site", "")
        campaign = kwargs.get("campaign", "")

        # Example implementation
        if site and campaign:
            return [Path("filecatalog") / campaign / site]
        elif site:
            return Path("filecatalog") / site
        else:
            return None


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

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = glob.glob(str(job_path / "*.sim"))
        if outputs:
            self._store_output("sim", outputs[0])


class PiSimulate_v2(IMetadataModel):
    """Pi simulation metadata model."""

    num_points: int
    output_path: str

    def get_output_query(self, output_name: str) -> Path | None:
        if output_name == "sim":
            return Path("filecatalog") / "pi" / str(self.num_points)
        return None

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = job_path / self.output_path
        if outputs:
            self._store_output("sim", str(outputs))


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
            return Path("filecatalog") / "pi" / str(self.num_points * len(self.input_data))
        return None

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = glob.glob(str(job_path / "*.sim"))
        if outputs:
            self._store_output("pi_result", outputs[0])


# -----------------------------------------------------------------------------


class LHCbSimulate(IMetadataModel):
    """LHCb metadata model version 1."""

    task_id: int
    run_id: int
    number_of_events: int

    def get_input_query(self, input_name: str) -> Path | None:
        return Path("filecatalog") / str(self.task_id) / str(self.run_id)

    def get_output_query(self, output_name: str) -> Path | None:
        return Path("filecatalog") / str(self.task_id) / str(self.run_id)

    def pre_process(self, job_path: Path, command: List[str]) -> List[str]:
        """Pre process the inputs of a job.

        - Open the file specified in the last argument of the command: it always contains the parameters
        - Compute the number of events to simulate based on the cpu power (fake it) and the number of processors
          available (from the requirements of the cwl of default is 1)
        """
        # Load the document, at this point we know the document is valid
        task = load_document_by_uri(job_path / command[1])
        cores_min = None
        cores_max = None
        for requirement in task.requirements:
            if requirement.class_ == "ResourceRequirement":
                cores_min = requirement.coresMin
                cores_max = requirement.coresMax

        if cores_min is None:
            cores_min = 1
        if cores_max is None:
            cores_max = 1

        # Get the number of processors to use
        number_of_processors_wn = random.randint(1, 4)
        number_of_processors = 1
        if number_of_processors_wn < cores_min:
            raise RuntimeError("Not enough processors available.")
        if number_of_processors_wn > cores_max:
            number_of_processors = cores_max
        else:
            number_of_processors = number_of_processors_wn

        # Get the CPU power from the local config (fake it with a random number)
        cpu_power = random.randint(10, 20)
        cpu_time = random.randint(3600, 86400)
        cpu_work_per_event = random.randint(600, 1200)
        self.number_of_events = (
            math.floor((cpu_power * cpu_time) / cpu_work_per_event)
            * number_of_processors
        )

        # Write the number of events to simulate in the last argument of the command
        if len(command) == 3:
            parameters_path = job_path / command[2]
            parameters = load_inputfile(str(parameters_path))
        else:
            parameters_path = job_path / "parameter.cwl"
            parameters = {}
            command.append(parameters_path.name)
        parameters["number-of-events"] = self.number_of_events

        # Save the parameters to the file
        parameter_dict = save(cast(Saveable, parameters))
        with open(parameters_path, "w") as parameter_file:
            YAML().dump(parameter_dict, parameter_file)

        return command

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = glob.glob(str(job_path / "*.sim"))
        if outputs:
            self._store_output("sim", outputs[0])
        outputs = glob.glob(str(job_path / "pool_xml_catalog.xml"))
        if outputs:
            self._store_output("pool_xml_catalog", outputs[0])


class LHCbReconstruct(IMetadataModel):
    """LHCb metadata model version 1."""

    task_id: int
    run_id: int
    # Input data
    files: List | None

    def get_input_query(self, input_name: str) -> Path | None:
        return Path("filecatalog") / str(self.task_id) / str(self.run_id)

    def get_output_query(self, output_name: str) -> Path | None:
        return Path("filecatalog") / str(self.task_id) / str(self.run_id)

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = glob.glob(str(job_path / "*.sim"))
        if outputs:
            self._store_output("sim", outputs[0])
        outputs = glob.glob(str(job_path / "pool_xml_catalog.xml"))
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
            return Path("filecatalog") / "mandelbrot" / "images" / "raw" / f"{self.width}x{self.height}"
        return None

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = glob.glob(str(job_path / "data*.txt"))
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
            return Path("filecatalog") / "mandelbrot" / "images" / "merged" / f"{width}x{height}"
        return None

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = glob.glob(str(job_path / "mandelbrot_image*bmp"))
        if outputs:
            self._store_output("data-merged", outputs[0])


class DataGenerationModel(IMetadataModel):
    """Data generation metadata model."""

    output_file_name_1: str | None
    output_file_name_2: str | None

    def get_output_query(self, output_name: str) -> Path | None:
        if self.output_file_name_1 and output_name == "data1":
            return Path("filecatalog") / "gaussian_fit" / "data-generation-1"
        if self.output_file_name_2 and output_name == "data2":
            return Path("filecatalog") / "gaussian_fit" / "data-generation-2"
        return Path("filecatalog") / "gaussian_fit" / "data-generation"

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = glob.glob(
            str(job_path / self.output_file_name_1) if self.output_file_name_1 else "*"
        )
        if outputs:
            self._store_output("data1", outputs[0])
        outputs = glob.glob(
            str(job_path / self.output_file_name_2) if self.output_file_name_2 else "*"
        )
        if outputs:
            self._store_output("data2", outputs[0])


class GaussianFitModel(IMetadataModel):
    """Gaussian Fit metadata model."""

    # Input data
    data1: List | None
    data2: List | None

    def get_input_query(self, input_name: str) -> Path | None:
        base_path = Path("filecatalog") / "gaussian_fit"
        if input_name == "data1":
            return base_path / "data-generation-1"
        if input_name == "data2":
            return base_path / "data-generation-2"
        return None

    def get_output_query(self, output_name: str) -> Path | None:
        if output_name == "fit-data" and self.data1 and self.data2:
            return Path("filecatalog") / "gaussian_fit" / "fit"
        return None

    def post_process(self, job_path: Path):
        """Post process the outputs of a job."""
        outputs = glob.glob(str(job_path / "fit*"))
        if outputs:
            self._store_output("fit-data", outputs[0])

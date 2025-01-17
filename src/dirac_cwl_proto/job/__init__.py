"""
CLI interface to run a workflow as a job.
"""

import logging
import random
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import typer
from cwl_utils.pack import pack
from cwl_utils.parser import load_document, save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    File,
    Saveable,
    Workflow,
)
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from rich import print_json
from rich.console import Console
from rich.text import Text
from ruamel.yaml import YAML
from schema_salad.exceptions import ValidationException

from dirac_cwl_proto.submission_models import (
    JobDescriptionModel,
    JobMetadataModel,
    JobParameterModel,
    JobSubmissionModel,
)
from dirac_cwl_proto.utils import _get_metadata

app = typer.Typer()
console = Console()

# -----------------------------------------------------------------------------
# dirac-cli commands
# -----------------------------------------------------------------------------


@app.command("submit")
def submit_job_client(
    task_path: str = typer.Argument(..., help="Path to the CWL file"),
    parameter_path: Optional[List[str]] = typer.Option(None, help="Path to the files containing the metadata"),
    metadata_path: Optional[str] = typer.Option(None, help="Path to metadata file used to generate the input query"),
    platform: Optional[str] = typer.Option(None, help="The platform required to run the job"),
    priority: Optional[int] = typer.Option(10, help="The priority of the job"),
    sites: Optional[List[str]] = typer.Option(None, help="The site to run the job"),
    # Specific parameter for the purpose of the prototype
    local: Optional[bool] = typer.Option(True, help="Run the job locally instead of submitting it to the router"),
):
    """
    Correspond to the dirac-cli command to submit jobs

    This command will:
    - Validate the workflow
    - Start the jobs
    """
    # Validate the workflow
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Validating the job(s)...")
    try:
        task = load_document(pack(task_path))
    except FileNotFoundError as ex:
        console.print(
            f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to load the task:\n{ex}"
        )
        return typer.Exit(code=1)
    except ValidationException as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to validate the task:\n{ex}")
        return typer.Exit(code=1)

    console.print(f"\t[green]:heavy_check_mark:[/green] Task {task_path}")

    job_metadata = JobMetadataModel()
    job_description = JobDescriptionModel(
        platform=platform,
        priority=priority,
        sites=sites,
    )
    if task.hints:
        for hint in task.hints:
            hint_class = hint["class"]
            hint_stripped = {k: v for k, v in hint.items() if k != "class"}
            if hint_class == "dirac:metadata":
                console.print(f"Update metadata with:\n{hint_stripped}")
                job_metadata = job_metadata.copy(update=hint_stripped)
                continue
            if hint_class == "dirac:description":
                job_description = job_description.copy(update=hint_stripped)

    console.print("\t[green]:heavy_check_mark:[/green] Metadata")
    console.print("\t[green]:heavy_check_mark:[/green] Description")

    parameters = []
    if parameter_path:
        for parameter_p in parameter_path:
            parameter = load_inputfile(parameter_p)

            overrides = parameter.pop("cwltool:overrides", {})
            if overrides:
                if len(overrides) > 1:
                    # QUESTION: What's the best way to handle a ValueError? Raising it or logging and exiting wiht 1:
                    # console.print(
                    #     "[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] "
                    #     "Job submission model only supports one override per parameter."
                    # )
                    # return typer.Exit(code=1)
                    raise ValueError("Job submission model only supports one override per parameter.")
                override_hints = overrides[next(iter(overrides))].get("hints", {})
                if override_hints:
                    job_description = job_description.copy(update=override_hints.pop("dirac:description", {}))
                    job_metadata = job_metadata.copy(update=override_hints.pop("dirac:metadata", {}))

            # Upload the local files to the sandbox store
            sandbox_id = upload_local_input_files(parameter)

            parameters.append(
                JobParameterModel(
                    sandbox=[sandbox_id] if sandbox_id else None,
                    cwl=parameter,
                )
            )
            console.print(f"\t[green]:heavy_check_mark:[/green] Parameter {parameter_p}")

    job = JobSubmissionModel(
        task=task,
        parameters=parameters,
        description=job_description,
        metadata=job_metadata,
    )
    console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Job(s) validated.")

    # Submit the job
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Submitting the job(s) to service...")
    print_json(job.model_dump_json(indent=4))
    if not submit_job_router(job):
        console.print("[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to run job(s).")
        return typer.Exit(code=1)
    console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Job(s) done.")


def upload_local_input_files(input_data: Dict[str, Any]) -> str | None:
    """
    Extract the files from the parameters.

    :param parameters: The parameters of the job

    :return: The list of files
    """
    Path("sandboxstore").mkdir(exist_ok=True)

    # Get the files from the input data
    files = []
    for _, input_value in input_data.items():
        if isinstance(input_value, list):
            for item in input_value:
                if isinstance(item, File):
                    files.append(item)
        elif isinstance(input_value, File):
            files.append(input_value)

    if not files:
        return None

    # Tar the files and upload them to the file catalog
    sandbox_path = Path("sandboxstore") / f"input_sandbox_{random.randint(1000, 9999)}.tar.gz"
    with tarfile.open(sandbox_path, "w:gz") as tar:
        for file in files:
            # TODO: path is not the only attribute to consider, but so far it is the only one used
            if not file.path:
                raise NotImplementedError("File path is not defined.")

            file_path = Path(file.path.replace("file://", ""))
            console.print(
                f"\t\t[blue]:information_source:[/blue] Found {file_path} locally, uploading it to the sandbox store..."
            )
            tar.add(file_path, arcname=file_path.name)
    console.print(f"\t\t[blue]:information_source:[/blue] File(s) will be available through {sandbox_path}")

    # Modify the location of the files to point to the future location on the worker node
    for file in files:
        # TODO: path is not the only attribute to consider, but so far it is the only one used
        if not file.path:
            raise NotImplementedError("File path is not defined.")

        file.path = str(Path(".") / file.path.split("/")[-1])

    sandbox_id = sandbox_path.name.replace(".tar.gz", "")
    return sandbox_id


# -----------------------------------------------------------------------------
# dirac-router commands
# -----------------------------------------------------------------------------


def submit_job_router(job: JobSubmissionModel) -> bool:
    """
    Execute a job using the router.

    :param job: The task to execute

    :return: True if the job executed successfully, False otherwise
    """
    logger = logging.getLogger("JobRouter")

    # Validate the jobs
    logger.info("Validating the job(s)...")
    # Initiate 1 job per parameter
    jobs = []
    if not job.parameters:
        jobs.append(job)
    else:
        for parameter in job.parameters:
            jobs.append(
                JobSubmissionModel(
                    task=job.task,
                    parameters=[parameter],
                    description=job.description,
                    metadata=job.metadata,
                )
            )
    logger.info("Job(s) validated!")

    # Simulate the submission of the job (just execute the job locally)
    logger.info("Running jobs...")
    results = []
    for job in jobs:
        logger.info("Running job:\n")
        print_json(job.model_dump_json(indent=4))
        results.append(run_job(job))
    logger.info("Jobs done.")

    return all(results)


# -----------------------------------------------------------------------------
# Job Execution Coordinator
# -----------------------------------------------------------------------------


class JobExecutionCoordinator:
    """Reproduction of the JobExecutionCoordinator.

    In Dirac, you would inherit from it to define your pre/post-processing strategy.
    In this context, we assume that these stages depend on the JobType.
    """

    def __init__(self, job: JobSubmissionModel):
        # Get a metadata instance
        self.metadata = _get_metadata(job)

    def pre_process(self, job_path: Path, command: List[str]) -> List[str]:
        """Pre process a job according to its type."""
        if self.metadata:
            return self.metadata.pre_process(job_path, command)

        return command

    def post_process(self, job_path: Path) -> bool:
        """Post process a job according to its type."""
        if self.metadata:
            return self.metadata.post_process(job_path)

        return True


# -----------------------------------------------------------------------------
# JobWrapper
# -----------------------------------------------------------------------------


def _pre_process(
    executable: CommandLineTool | Workflow,
    arguments: JobParameterModel | None,
    job_exec_coordinator: JobExecutionCoordinator,
    job_path: Path,
) -> List[str]:
    """
    Pre-process the job before execution.

    :return: True if the job is pre-processed successfully, False otherwise
    """
    logger = logging.getLogger("JobWrapper - Pre-process")

    # Prepare the task for cwltool
    logger.info("Preparing the task for cwltool...")
    command = ["cwltool"]

    task_dict = save(executable)
    task_path = job_path / "task.cwl"
    with open(task_path, "w") as task_file:
        YAML().dump(task_dict, task_file)
    command.append(str(task_path.name))

    if arguments:
        if arguments.sandbox:
            # Download the files from the sandbox store
            logger.info("Downloading the files from the sandbox store...")
            for sandbox in arguments.sandbox:
                sandbox_path = Path("sandboxstore") / f"{sandbox}.tar.gz"
                with tarfile.open(sandbox_path, "r:gz") as tar:
                    tar.extractall(job_path)
            logger.info("Files downloaded successfully!")

        # Download input data from the file catalog
        logger.info("Downloading input data from the file catalog...")
        input_data = []
        for _, input_value in arguments.cwl.items():
            input = input_value
            if not isinstance(input_value, list):
                input = [input_value]

            for item in input:
                if not isinstance(item, File):
                    continue

                # TODO: path is not the only attribute to consider, but so far it is the only one used
                if not item.path:
                    raise NotImplementedError("File path is not defined.")

                input_path = Path(item.path)
                if "filecatalog" in input_path.parts:
                    input_data.append(item)

        for file in input_data:
            # TODO: path is not the only attribute to consider, but so far it is the only one used
            if not file.path:
                raise NotImplementedError("File path is not defined.")

            input_path = Path(file.path)
            shutil.copy(input_path, job_path / input_path.name)
            file.path = file.path.split("/")[-1]
        logger.info("Input data downloaded successfully!")

        # Prepare the parameters for cwltool
        logger.info("Preparing the parameters for cwltool...")
        parameter_dict = save(cast(Saveable, arguments.cwl))
        parameter_path = job_path / "parameter.cwl"
        with open(parameter_path, "w") as parameter_file:
            YAML().dump(parameter_dict, parameter_file)
        command.append(str(parameter_path.name))
    return job_exec_coordinator.pre_process(job_path, command)


def _post_process(
    status: int,
    stdout: str,
    stderr: str,
    job_path: Path,
    job_exec_coordinator: JobExecutionCoordinator,
):
    """
    Post-process the job after execution.

    :return: True if the job is post-processed successfully, False otherwise
    """
    logger = logging.getLogger("JobWrapper - Post-process")
    if status != 0:
        raise RuntimeError(f"Error {status} during the task execution.")

    logger.info(stdout)
    logger.info(stderr)

    job_exec_coordinator.post_process(job_path)


def run_job(job: JobSubmissionModel) -> bool:
    """
    Executes a given CWL workflow using cwltool.
    This is the equivalent of the DIRAC JobWrapper.

    :return: True if the job is executed successfully, False otherwise
    """
    logger = logging.getLogger("JobWrapper")
    job_exec_coordinator = JobExecutionCoordinator(job)

    # Isolate the job in a specific directory
    job_path = Path(".") / "workernode" / f"{random.randint(1000, 9999)}"
    job_path.mkdir(parents=True, exist_ok=True)

    try:
        # Pre-process the job
        logger.info("Pre-processing Task...")
        command = _pre_process(
            job.task,
            job.parameters[0] if job.parameters else None,
            job_exec_coordinator,
            job_path,
        )
        logger.info("Task pre-processed successfully!")

        # Execute the task
        logger.info(f"Executing Task: {command}")
        result = subprocess.run(command, capture_output=True, text=True, cwd=job_path)

        if result.returncode != 0:
            logger.error(f"Error in executing workflow:\n{Text.from_ansi(result.stderr)}")
            return False
        logger.info("Task executed successfully!")

        # Post-process the job
        logger.info("Post-processing Task...")
        _post_process(
            result.returncode,
            result.stdout,
            result.stderr,
            job_path,
            job_exec_coordinator,
        )
        logger.info("Task post-processed successfully!")
        return True

    except Exception:
        logger.exception("JobWrapper: Failed to execute workflow")
        return False
    finally:
        # Clean up
        if job_path.exists():
            shutil.rmtree(job_path)

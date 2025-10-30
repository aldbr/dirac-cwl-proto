"""
CLI interface to run a workflow as a job.
"""

import json
import logging
import random
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import Any

import typer
from cwl_utils.pack import pack
from cwl_utils.parser import load_document, save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    File,
    Workflow,
)
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from rich import print_json
from rich.console import Console
from rich.text import Text
from ruamel.yaml import YAML
from schema_salad.exceptions import ValidationException

from dirac_cwl_proto.execution_hooks.core import ExecutionHooksBasePlugin
from dirac_cwl_proto.submission_models import (
    JobInputModel,
    JobSubmissionModel,
    extract_dirac_hints,
)

app = typer.Typer()
console = Console()

# -----------------------------------------------------------------------------
# dirac-cli commands
# -----------------------------------------------------------------------------


@app.command("submit")
def submit_job_client(
    task_path: str = typer.Argument(..., help="Path to the CWL file"),
    parameter_path: list[str]
    | None = typer.Option(None, help="Path to the files containing the metadata"),
    # Specific parameter for the purpose of the prototype
    local: bool
    | None = typer.Option(
        True, help="Run the job locally instead of submitting it to the router"
    ),
):
    """
    Correspond to the dirac-cli command to submit jobs

    This command will:
    - Validate the workflow
    - Start the jobs
    """
    # Validate the workflow
    console.print(
        "[blue]:information_source:[/blue] [bold]CLI:[/bold] Validating the job(s)..."
    )
    try:
        task = load_document(pack(task_path))
    except FileNotFoundError as ex:
        console.print(
            f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to load the task:\n{ex}"
        )
        return typer.Exit(code=1)
    except ValidationException as ex:
        console.print(
            f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to validate the task:\n{ex}"
        )
        return typer.Exit(code=1)

    console.print(f"\t[green]:heavy_check_mark:[/green] Task {task_path}")

    # Extract and validate dirac hints; unknown hints are logged as warnings.
    try:
        job_metadata, job_scheduling = extract_dirac_hints(task)
    except Exception as exc:
        console.print(
            f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Invalid DIRAC hints:\n{exc}"
        )
        return typer.Exit(code=1)

    console.print("\t[green]:heavy_check_mark:[/green] Metadata")
    console.print("\t[green]:heavy_check_mark:[/green] Description")

    parameters = []
    if parameter_path:
        for parameter_p in parameter_path:
            try:
                parameter = load_inputfile(parameter_p)
            except Exception as ex:
                console.print(
                    f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to validate the parameter:\n{ex}"
                )
                return typer.Exit(code=1)

            overrides = parameter.pop("cwltool:overrides", {})
            if overrides:
                override_hints = overrides[next(iter(overrides))].get("hints", {})
                if override_hints:
                    job_scheduling = job_scheduling.model_copy(
                        update=override_hints.pop("dirac:scheduling", {})
                    )
                    job_metadata = job_metadata.model_copy(
                        update=override_hints.pop("dirac:execution-hooks", {})
                    )

            # Upload the local files to the sandbox store
            sandbox_id = upload_local_input_files(parameter)
            lfns = get_lfns(parameter)

            parameters.append(
                JobInputModel(
                    sandbox=[sandbox_id] if sandbox_id else None,
                    cwl=parameter,
                    lfns_input=lfns,
                )
            )
            console.print(
                f"\t[green]:heavy_check_mark:[/green] Parameter {parameter_p}"
            )

    job = JobSubmissionModel(
        task=task,
        parameters=parameters,
        scheduling=job_scheduling,
        execution_hooks=job_metadata,
    )
    console.print(
        "[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Job(s) validated."
    )

    # Submit the job
    console.print(
        "[blue]:information_source:[/blue] [bold]CLI:[/bold] Submitting the job(s) to service..."
    )
    print_json(job.model_dump_json(indent=4))
    if not submit_job_router(job):
        console.print(
            "[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to run job(s)."
        )
        return typer.Exit(code=1)
    console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Job(s) done.")


def upload_local_input_files(input_data: dict[str, Any]) -> str | None:
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
    sandbox_path = (
        Path("sandboxstore") / f"input_sandbox_{random.randint(1000, 9999)}.tar.gz"
    )
    with tarfile.open(sandbox_path, "w:gz") as tar:
        for file in files:
            # TODO: path is not the only attribute to consider, but so far it is the only one used
            if not file.path and not file.location:
                raise NotImplementedError("File path is not defined.")
            # Skip files from the File Catalog
            if file.location and file.location.startswith("lfn:"):
                continue

            if not file.location:
                file.location = file.path

            file_path = Path(file.location.replace("file://", ""))
            console.print(
                f"\t\t[blue]:information_source:[/blue] Found {file_path} locally, uploading it to the sandbox store..."
            )
            tar.add(file_path, arcname=file_path.name)
    console.print(
        f"\t\t[blue]:information_source:[/blue] File(s) will be available through {sandbox_path}"
    )

    # Modify the location of the files to point to the future location on the worker node
    for file in files:
        # TODO: path is not the only attribute to consider, but so far it is the only one used
        if not file.location and not file.path:
            raise NotImplementedError("File path is not defined.")

        if file.path:
            file.path = str(Path(".") / file.path.split("/")[-1])

    sandbox_id = sandbox_path.name.replace(".tar.gz", "")
    return sandbox_id


def get_lfns(input_data: dict[str, Any]) -> dict[str, Path | list[Path]]:
    """
    Get the list au LFNs in the inputs from the parameters

    :param input_data: The parameters of the job
    :return: The list of LFN paths
    """
    # Get the files from the input data
    files: dict[str, Path | list[Path]] = {}
    for input_name, input_value in input_data.items():
        if isinstance(input_value, list):
            val = []
            for item in input_value:
                if isinstance(item, File):
                    if not item.location:
                        raise NotImplementedError("File location is not defined.")
                    # Skip files from the File Catalog
                    if item.location.startswith("lfn:"):
                        val.append(Path(item.location))
            files[input_name] = val
        elif isinstance(input_value, File):
            if not input_value.location:
                raise NotImplementedError("File location is not defined.")
            if input_value.location.startswith("lfn:"):
                files[input_name] = Path(input_value.location)
    return files


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
                    scheduling=job.scheduling,
                    execution_hooks=job.execution_hooks,
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
# JobWrapper
# -----------------------------------------------------------------------------


def _pre_process(
    executable: CommandLineTool | Workflow | ExpressionTool,
    arguments: JobInputModel | None,
    runtime_metadata: ExecutionHooksBasePlugin | None,
    job_path: Path,
) -> list[str]:
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
                    tar.extractall(job_path, filter="data")
            logger.info("Files downloaded successfully!")

    if runtime_metadata:
        return runtime_metadata.pre_process(executable, arguments, job_path, command)

    return command


def _post_process(
    status: int,
    stdout: str,
    stderr: str,
    job_path: Path,
    runtime_metadata: ExecutionHooksBasePlugin | None,
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

    outputs = json.loads(stdout)
    sandboxes = upload_workernode_output_files(outputs)
    
    console.print(
            f"\t[blue]:information_source:[/blue] {len(sandboxes)} Sandbox(es) will be available through: \n\t\t- {
                "\n\t\t- ".join(str(sb) for sb in sandboxes)
                }"
        )

    if runtime_metadata:
        return runtime_metadata.post_process(job_path)

    return True

def upload_workernode_output_files(outputs: dict[str, Any]) -> list[Path]:
    
    Path("sandboxstore").mkdir(exist_ok=True)
    
    sandbox_id = random.randint(1000, 9999)
    sandboxes = []
    
    for output, files in outputs.items():
        sandbox_path = Path("sandboxstore") / f"output_sandbox_{sandbox_id}_{output}.tar.gz"
        sandboxes.append(sandbox_path)
        if not isinstance(files, list):
            files = [files]
        with tarfile.open(sandbox_path, "w:gz") as tar:
            for file in files:
                if not file:
                    break
                
                file_path = Path(file["path"].replace("file://", ""))
                console.print(
                    f"\t\t[blue]:information_source:[/blue] Found {file_path} on the worker node, uploading it to the sandbox store..."
                )
                tar.add(file_path, arcname=file_path.name)
        console.print(
            f"\t\t[blue]:information_source:[/blue] File(s) will be available through {sandbox_path}"
        )
        
    return sandboxes


def run_job(job: JobSubmissionModel) -> bool:
    """
    Executes a given CWL workflow using cwltool.
    This is the equivalent of the DIRAC JobWrapper.

    :return: True if the job is executed successfully, False otherwise
    """
    logger = logging.getLogger("JobWrapper")
    # Instantiate runtime metadata from the serializable descriptor and
    # the job context so implementations can access task inputs/overrides.
    runtime_metadata = (
        job.execution_hooks.to_runtime(job) if job.execution_hooks else None
    )

    # Isolate the job in a specific directory
    job_path = Path(".") / "workernode" / f"{random.randint(1000, 9999)}"
    job_path.mkdir(parents=True, exist_ok=True)

    try:
        # Pre-process the job
        logger.info("Pre-processing Task...")
        command = _pre_process(
            job.task,
            job.parameters[0] if job.parameters else None,
            runtime_metadata,
            job_path,
        )
        logger.info("Task pre-processed successfully!")

        # Execute the task
        logger.info(f"Executing Task: {command}")
        result = subprocess.run(command, capture_output=True, text=True, cwd=job_path)

        if result.returncode != 0:
            logger.error(
                f"Error in executing workflow:\n{Text.from_ansi(result.stderr)}"
            )
            return False
        logger.info("Task executed successfully!")

        # Post-process the job
        logger.info("Post-processing Task...")
        if _post_process(
            result.returncode,
            result.stdout,
            result.stderr,
            job_path,
            runtime_metadata,
        ):
            logger.info("Task post-processed successfully!")
            return True
        logger.error("Failed to post-process Task")
        return False

    except Exception:
        logger.exception("JobWrapper: Failed to execute workflow")
        return False
    finally:
        # Clean up
        if job_path.exists():
            shutil.rmtree(job_path)

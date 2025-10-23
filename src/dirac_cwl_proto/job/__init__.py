"""
CLI interface to run a workflow as a job.
"""

import logging
import random
import tarfile
from pathlib import Path
from typing import Any

import typer
from cwl_utils.pack import pack
from cwl_utils.parser import load_document
from cwl_utils.parser.cwl_v1_2 import (
    File,
)
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from rich import print_json
from rich.console import Console
from schema_salad.exceptions import ValidationException

from dirac_cwl_proto.job.job_wrapper import JobWrapper
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
    sandbox_id = None
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

            # Upload input files to the local sandbox store
            if local:
                sandbox_id = upload_local_input_files(parameter)

            parameters.append(
                JobInputModel(
                    sandbox=[sandbox_id] if sandbox_id else None,
                    cwl=parameter,
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

    jobs = validate_jobs(job)

    for job in jobs:
        # Dump the job model to a file
        with open("job.json", "w") as f:
            f.write(job.model_dump_json())

        # TODO add call to create_sandbox router adding files from parameter and the job.json file
        # For now just set hardcoded sandbox_id
        sandbox_id = "SB:SandboxSE|/S3/diracx-sandbox-store/isb.tar.bz2"

        # Convert job.jspn to jdl
        console.print(
            "[blue]:information_source:[/blue] [bold]CLI:[/bold] Converting job model to jdl..."
        )
        convert_to_jdl(job, sandbox_id)

    # Submit the job
    console.print(
        "[blue]:information_source:[/blue] [bold]CLI:[/bold] Submitting the job(s)..."
    )
    print_json(job.model_dump_json(indent=4))

    if local:
        if not submit_job_router(job):
            console.print(
                "[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to run job(s)."
            )
            return typer.Exit(code=1)
        console.print(
            "[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Job(s) done."
        )
    else:
        # TODO call job/jdl router
        console.print(
            "[blue]:information_source:[/blue] [bold]CLI:[/bold] Call diracx: jobs/jdl router..."
        )


def validate_jobs(job: JobSubmissionModel) -> list[JobSubmissionModel]:
    console.print(
        "[blue]:information_source:[/blue] [bold]CLI:[/bold] Validating the job(s)..."
    )
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
    console.print(
        "[green]:information_source:[/green] [bold]CLI:[/bold] Job(s) validated!"
    )
    return jobs


def convert_to_jdl(job: JobSubmissionModel, sandbox_id: str) -> None:
    """
    Convert job model to jdl.

    :param job: The task to execute

    :param sandbox_id: The sandbox id
    """
    with open("generated.jdl", "w") as f:
        f.write("Executable = job_wrapper_template.py;\n")
        f.write("Arguments = job.json;\n")
        f.write("CPUTime = 86400;\n")
        f.write("JobName = test;\n")
        f.write(
            """OutputSandbox =
        {
            std.out,
            std.err
        };\n"""
        )
        if job.scheduling.priority:
            f.write(f"Priority = {job.scheduling.priority};\n")
        if job.scheduling.sites:
            f.write(f"Site = {job.scheduling.sites};\n")
        f.write(f"InputSandbox = {sandbox_id};\n")
    return None


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
            if not file.path:
                raise NotImplementedError("File path is not defined.")

            file_path = Path(file.path.replace("file://", ""))
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
    jobs = validate_jobs(job)

    # Execute the job locally
    logger.info("Executing jobs locally...")
    results = []

    for job in jobs:
        job_wrapper = JobWrapper()
        logger.info("Executing job locally:\n")
        print_json(job.model_dump_json(indent=4))
        results.append(job_wrapper.run_job(job))
        logger.info("Jobs done.")

    return all(results)

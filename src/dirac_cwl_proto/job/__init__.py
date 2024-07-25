"""
CLI interface to run a workflow as a job.
"""
import logging
import subprocess

import typer
from rich.console import Console
from rich.text import Text

from dirac_cwl_proto.utils import CWLBaseModel

app = typer.Typer()
console = Console()


@app.command("submit")
def run(
    workflow_path: str = typer.Argument(..., help="Path to the CWL file"),
    metadata_path: str = typer.Argument(
        ..., help="Path to the file containing the metadata"
    ),
    metadata_type: str = typer.Argument(
        ..., help="Type of metadata to use", case_sensitive=False
    ),
):
    """
    Run a workflow from end to end (production/transformation/job).

    This command will:
    - Validate the workflow
    - Start the jobs
    """
    # Start the production
    console.print("[blue]:information_source:[/blue] [bold]Starting the job...[/bold]")
    if not submit_job(workflow_path, metadata_path, metadata_type):
        console.print(
            "[red]:heavy_multiplication_x:[/red] [bold]Failed to run production.[/bold]"
        )
        return typer.Exit(code=1)
    console.print("[green]:heavy_check_mark:[/green] [bold]Job done.[/bold]")


# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------


class JobModel(CWLBaseModel):
    """A job is a single execution of a transformation."""

    pass


# -----------------------------------------------------------------------------
# Job management
# -----------------------------------------------------------------------------


def submit_job(workflow_path: str, metadata_path: str, metadata_type: str) -> bool:
    """
    Executes a given CWL workflow using cwltool.
    This is the equivalent of the DIRAC JobWrapper.

    :param workflow_path: Path to the CWL workflow
    :param metadata_path: Path to the metadata file
    :param metadata_type: Type of metadata to use

    :return: True if the workflow executed successfully, False otherwise
    """
    job = JobModel(
        workflow_path=workflow_path,
        metadata_path=metadata_path,
        metadata_type=metadata_type,
    )

    try:
        logging.info(f"Executing workflow: {workflow_path}")
        result = subprocess.run(
            ["cwltool", workflow_path, metadata_path], capture_output=True, text=True
        )

        if result.returncode != 0:
            logging.error(
                f"Error in executing workflow:\n{Text.from_ansi(result.stderr)}"
            )
            return False

        if not job.metadata:
            # This should never happen
            logging.error(
                "Error in executing workflow:\nNo metadata attached to the job"
            )
            return False

        job.metadata.post_process()

        logging.info("Workflow executed successfully.")
        return True

    except Exception:
        logging.exception("Failed to execute workflow")
        return False

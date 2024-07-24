"""
CLI interface to run a workflow as a job.
"""
import subprocess

import typer
from rich.console import Console
from rich.text import Text

from dirac_cwl_proto.utils import CWLBaseModel

app = typer.Typer()
console = Console()


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
        console.print(f"Executing workflow: [yellow]{workflow_path}[/yellow]")
        result = subprocess.run(
            ["cwltool", workflow_path, metadata_path], capture_output=True, text=True
        )

        if result.returncode != 0:
            console.print(
                f":x: [red]Error in executing workflow:[/red] \n{Text.from_ansi(result.stderr)}"
            )
            return False

        if not job.metadata:
            # This should never happen
            console.print(
                ":x: [red]Error in executing workflow:[/red] \nNo metadata attached to the job"
            )
            return False

        job.metadata.post_process()

        console.print(
            "[green]:heavy_check_mark: Workflow executed successfully.[/green]"
        )
        return True

    except Exception as e:
        console.print(f":x: [red]Failed to execute workflow: {e}[/red]")
        return False

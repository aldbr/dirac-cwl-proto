import logging
import subprocess

from rich.console import Console
from rich.text import Text

console = Console()


def validate_cwl(cwl_file: str):
    """Validates a CWL file."""
    result = subprocess.run(
        ["cwltool", "--validate", cwl_file], capture_output=True, text=True
    )
    if result.returncode == 0:
        return True

    logging.error(Text.from_ansi(result.stderr))
    return False


def dash_to_snake_case(name):
    """Converts a string from dash-case to snake_case."""
    return name.replace("-", "_")


def snake_case_to_dash(name):
    """Converts a string from snake_case to dash-case."""
    return name.replace("_", "-")

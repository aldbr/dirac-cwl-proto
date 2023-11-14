import logging
import subprocess
from rich.console import Console
from rich.text import Text

console = Console()

def validate_cwl(cwl_file: str):
    """Validates a CWL file."""
    result = subprocess.run(["cwltool", "--validate", cwl_file], capture_output=True, text=True)
    if result.returncode == 0:
        return True

    logging.error(Text.from_ansi(result.stderr))
    return False


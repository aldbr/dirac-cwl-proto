import subprocess
from rich.console import Console
from rich.text import Text

console = Console()

def validate_cwl(cwl_file: str):
    """Validates a CWL file."""
    result = subprocess.run(["cwltool", "--validate", cwl_file], capture_output=True, text=True)
    if result.returncode == 0:
        console.print(":heavy_check_mark: [green]Validation successful[/green]")
        return True

    error_text = Text.from_ansi(result.stderr)
    console.print(f"[red]:heavy_multiplication_x:[/red] Validation failed\n{error_text}")
    return False


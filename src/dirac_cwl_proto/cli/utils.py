import cwltool
from rich.console import Console
from cwltool.main import main as cwl_main

console = Console()

def validate_cwl(cwl_file: str):
    """Validates a CWL file."""
    try:
        result = cwl_main(["--validate", cwl_file])
        if result == 0:
            console.print(":heavy_check_mark: [green]Validation successful[/green]")
            return True
        else:
            console.print(":x: [red]Validation failed[/red]")
            return False
    except Exception as e:
        console.print(f":warning: [yellow]Validation error: {e}[/yellow]")
        return False


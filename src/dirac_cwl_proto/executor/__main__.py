"""CLI tool for running CWL workflows with DIRAC executor.

This command-line tool runs CWL workflows using the DiracExecutor, which handles
replica catalog management for input and output files.
"""

import sys
import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler

from cwltool.main import main as cwltool_main
from cwltool.context import LoadingContext, RuntimeContext

from dirac_cwl_proto.executor import dirac_executor_factory

app = typer.Typer(
    help="Run CWL workflows with DIRAC replica catalog management",
    no_args_is_help=True,
)
console = Console()


@app.command()
def run(
    workflow: Path = typer.Argument(
        ...,
        help="Path to CWL workflow file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    inputs: Optional[Path] = typer.Option(
        None,
        "--inputs", "-i",
        help="Path to workflow inputs YAML/JSON file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    outdir: Path = typer.Option(
        Path("cwl_output"),
        "--outdir", "-o",
        help="Directory for workflow outputs",
    ),
    replica_catalog: Optional[Path] = typer.Option(
        None,
        "--replica-catalog", "-r",
        help="Path to master replica catalog JSON file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    tmpdir: Optional[Path] = typer.Option(
        None,
        "--tmpdir", "-t",
        help="Temporary directory for intermediate files",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose logging",
    ),
    debug: bool = typer.Option(
        False,
        "--debug", "-d",
        help="Enable debug logging",
    ),
    leave_tmpdir: bool = typer.Option(
        False,
        "--leave-tmpdir",
        help="Do not delete temporary directory after execution",
    ),
    parallel: bool = typer.Option(
        False,
        "--parallel", "-j",
        help="Enable parallel execution (requires cwltool support)",
    ),
):
    """
    Run a CWL workflow with DIRAC replica catalog management.

    This command executes CWL workflows using a custom DIRAC executor that:
    - Manages replica catalogs for input LFNs
    - Creates step-specific replica catalogs
    - Propagates output metadata back to the master catalog
    - Integrates with lb-prod-run wrapper for pool_xml_catalog generation

    Example:
        dirac-cwl-run workflow.cwl -i inputs.yml -r catalog.json -o output/
    """
    console.print("\n[bold cyan]=" * 80)
    console.print("[bold cyan]DIRAC CWL Workflow Executor")
    console.print("[bold cyan]=" * 80)
    console.print(f"[cyan]Workflow:[/cyan] {workflow}")
    if inputs:
        console.print(f"[cyan]Inputs:[/cyan] {inputs}")
    console.print(f"[cyan]Output directory:[/cyan] {outdir}")
    if replica_catalog:
        console.print(f"[cyan]Replica catalog:[/cyan] {replica_catalog}")
    console.print()

    # Create output directory
    outdir.mkdir(parents=True, exist_ok=True)

    # Build cwltool arguments
    cwltool_args = [
        str(workflow),
    ]

    # Add inputs file if provided
    if inputs:
        cwltool_args.append(str(inputs))

    # Configure cwltool via command-line arguments
    # We need to inject our custom executor
    args_with_options = [
        "--outdir", str(outdir),
    ]

    if tmpdir:
        args_with_options.extend(["--tmpdir-prefix", str(tmpdir)])

    if leave_tmpdir:
        args_with_options.append("--leave-tmpdir")

    if debug:
        args_with_options.append("--debug")
    elif verbose:
        args_with_options.append("--verbose")

    if parallel:
        args_with_options.append("--parallel")

    # Add the workflow and inputs at the end
    args_with_options.extend(cwltool_args)

    try:
        # Create our custom DIRAC executor with replica catalog support
        from dirac_cwl_proto.executor import DiracExecutor

        dirac_executor = DiracExecutor(master_catalog_path=replica_catalog)

        console.print("[bold green]✓[/bold green] [cyan]Using DIRAC executor with replica catalog management[/cyan]")
        if replica_catalog:
            console.print(f"[bold green]✓[/bold green] [cyan]  Master catalog: {replica_catalog}[/cyan]")
        else:
            console.print("[bold yellow]⚠[/bold yellow] [yellow]  No replica catalog provided - will create empty catalog[/yellow]")
        console.print()

        # Run cwltool with our custom executor
        # cwltool.main.main accepts an executor parameter
        # We need to provide a logger_handler to prevent logging setup errors
        import sys
        log_handler = logging.StreamHandler(sys.stderr)

        exit_code = cwltool_main(
            argsl=args_with_options,
            executor=dirac_executor,
            logger_handler=log_handler,
        )

        if exit_code == 0:
            console.print("\n[bold green]✅ Workflow executed successfully![/bold green]")
            console.print(f"[green]   Outputs available in: {outdir}[/green]")

            # Check if replica catalog was created/updated
            if replica_catalog:
                if replica_catalog.exists():
                    console.print(f"[green]   Replica catalog: {replica_catalog}[/green]")

                    # Show catalog stats
                    from dirac_cwl_proto.job.replica_catalog import ReplicaCatalog
                    try:
                        catalog = ReplicaCatalog.model_validate_json(replica_catalog.read_text())
                        console.print(f"[green]   Catalog entries: {len(catalog.root)}[/green]")
                    except Exception:
                        pass
            else:
                console.print("[dim]   Note: No master catalog was specified[/dim]")
        else:
            console.print(f"\n[bold red]❌ Workflow execution failed with exit code {exit_code}[/bold red]")

        return exit_code

    except Exception as e:
        console.print(f"\n[bold red]❌ Error executing workflow: {e}[/bold red]")
        if debug:
            console.print_exception()
        return 1


@app.command()
def version():
    """Show version information."""
    console.print("[bold]DIRAC CWL Executor[/bold]")
    console.print("Version: 0.1.0")
    console.print("Built with cwltool and DIRAC integration")


def main():
    """Entry point for the command-line tool."""
    app()


if __name__ == "__main__":
    main()

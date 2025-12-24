import yaml
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from . import fromProductionRequestYAMLToCWL

app = typer.Typer(help="Convert LHCb Production Request YAML files to CWL workflows")
console = Console()


def _sanitize_filename(name: str) -> str:
    """Sanitize a name to be filesystem-safe."""
    return name.replace("/", "_").replace(" ", "_").replace(":", "_").replace('#', '_')


@app.command()
def generate(
    yaml_file: Path = typer.Argument(
        ...,
        help="Path to the production request YAML file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    output_dir: Path = typer.Option(
        Path("generated"),
        "--output-dir", "-o",
        help="Directory where CWL files will be written",
    ),
    production_name: str = typer.Option(
        None,
        "--production", "-p",
        help="Specific production name to convert (default: convert all)",
    ),
    yaml_width: int = typer.Option(
        120,
        "--yaml-width", "-w",
        help="Maximum line width for generated YAML files",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output",
    ),
):
    """
    Generate CWL workflow files from LHCb production request YAML.

    This command converts production request YAML files into CWL workflow specifications.
    By default, it processes all productions in the YAML file. Use --production to convert
    a specific production.
    """
    from cwl_utils.parser import save
    from ruamel.yaml import YAML

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    console.print("\n[bold cyan]=" * 80)
    console.print("[bold cyan]LHCb Production Request to CWL Converter")
    console.print("[bold cyan]=" * 80)
    console.print(f"[cyan]Input:[/cyan] {yaml_file}")
    console.print(f"[cyan]Output directory:[/cyan] {output_dir}\n")

    # Load and parse YAML to discover all productions
    with open(yaml_file, "r") as f:
        productions_data = yaml.safe_load(f)

    # Handle multiple productions in one file
    if not isinstance(productions_data, list):
        productions_data = [productions_data]

    # Filter by production name if specified
    if production_name:
        productions_data = [p for p in productions_data if p.get("name") == production_name]
        if not productions_data:
            console.print(f"[red]❌ Production '{production_name}' not found in {yaml_file}[/red]")
            raise typer.Exit(1)

    generated_files = []
    skipped = []

    # Process each production
    for production in productions_data:
        prod_name = production.get("name", "unknown_production")
        production_type = production.get("type")

        # Check for required fields based on production type
        if production_type == "Simulation":
            event_types = production.get("event_types", [])
            if not event_types:
                msg = "no event_types found"
                console.print(f"[yellow]⚠️  {prod_name}: {msg}[/yellow]")
                skipped.append((prod_name, msg))
                continue
        elif production_type == "AnalysisProduction":
            # Analysis productions don't have event_types
            pass
        else:
            msg = f"type={production_type} (unsupported production type)"
            console.print(f"[yellow]⚠️  Skipping {prod_name}: {msg}[/yellow]")
            skipped.append((prod_name, msg))
            continue

        try:
            # Generate CWL workflow
            if verbose:
                console.print(f"[dim]Processing {prod_name}...[/dim]")

            workflow, inputs, metadata = fromProductionRequestYAMLToCWL(
                yaml_file,
                production_name=prod_name,
            )

            # Create filename
            sanitized_production = _sanitize_filename(prod_name)
            filename = f"{sanitized_production}.cwl"
            output_path = output_dir / filename

            # Convert workflow to dict
            workflow_dict = save(workflow)

            # Use ruamel.yaml to preserve LiteralScalarString formatting
            yaml_dumper = YAML()
            yaml_dumper.default_flow_style = False
            yaml_dumper.width = yaml_width

            with open(output_path, 'w') as f:
                yaml_dumper.dump(workflow_dict, f)

            # Store metadata for summary table
            if production_type == "Simulation":
                generated_files.append((prod_name, filename, len(workflow.steps), production_type, event_types))
            else:
                generated_files.append((prod_name, filename, len(workflow.steps), production_type, None))

            console.print(f"[green]✅ Generated:[/green] {filename}")
            if verbose:
                console.print(f"   [dim]Production: {prod_name}[/dim]")
                console.print(f"   [dim]Type: {production_type}[/dim]")
                if production_type == "Simulation" and event_types:
                    event_type_ids = [str(et.get('id', et)) for et in event_types]
                    console.print(f"   [dim]Event Types: {', '.join(event_type_ids)}[/dim]")
                console.print(f"   [dim]Steps: {len(workflow.steps)}[/dim]\n")

        except Exception as e:
            console.print(f"[red]❌ Error generating {prod_name}: {e}[/red]")
            if verbose:
                import traceback
                traceback.print_exc()
            skipped.append((prod_name, str(e)))

    # Print summary
    console.print("\n[bold cyan]=" * 80)
    console.print("[bold cyan]SUMMARY")
    console.print("[bold cyan]=" * 80)

    if generated_files:
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Production", style="cyan")
        table.add_column("File", style="green")
        table.add_column("Type", style="magenta")
        table.add_column("Steps", justify="right", style="yellow")
        table.add_column("Info", justify="left", style="blue")

        for prod_name, filename, num_steps, prod_type, event_types in generated_files:
            # Build info column based on production type
            if prod_type == "Simulation" and event_types:
                info = ", ".join(str(et['id']) for et in event_types)
            else:
                info = "-"

            table.add_row(
                prod_name,
                filename,
                prod_type,
                str(num_steps),
                info,
            )

        console.print(f"\n[green]Generated {len(generated_files)} CWL file(s) in {output_dir}[/green]")
        console.print(table)
    else:
        console.print("[yellow]No CWL files were generated[/yellow]")

    if skipped:
        console.print(f"\n[yellow]Skipped {len(skipped)} item(s):[/yellow]")
        for name, reason in skipped:
            console.print(f"  [dim]• {name}: {reason}[/dim]")

    console.print()


@app.command()
def list_productions(
    yaml_file: Path = typer.Argument(
        ...,
        help="Path to the production request YAML file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
):
    """
    List all productions available in a production request YAML file.
    """
    with open(yaml_file, "r") as f:
        productions_data = yaml.safe_load(f)

    # Handle multiple productions in one file
    if not isinstance(productions_data, list):
        productions_data = [productions_data]

    console.print(f"\n[bold cyan]Productions in {yaml_file}:[/bold cyan]\n")

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Event Types", justify="right", style="yellow")
    table.add_column("Steps", justify="right", style="blue")

    for production in productions_data:
        name = production.get("name", "unknown")
        prod_type = production.get("type", "unknown")
        event_types = production.get("event_types", [])
        steps = production.get("steps", [])

        table.add_row(
            name,
            prod_type,
            str(", ".join(str(et['id']) for et in event_types)),
            str(len(steps))
        )

    console.print(table)
    console.print(f"\n[dim]Total: {len(productions_data)} production(s)[/dim]\n")


def main():
    """Entry point for the command-line utility."""
    app()


if __name__ == "__main__":
    main()

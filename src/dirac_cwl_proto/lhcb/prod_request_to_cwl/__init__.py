"""LHCb Production Request to CWL converter.

This package provides converters for different LHCb production types:
- Simulation productions (simulation.py)
- Analysis productions (anaprod.py)

The main entry point automatically routes to the appropriate converter based on
the production type specified in the YAML file.
"""

from pathlib import Path
from typing import Any
import yaml

from .simulation import fromProductionRequestYAMLToCWL as fromSimulationToCWL
from .anaprod import fromProductionRequestYAMLToCWL as fromAnalysisToCWL


def main():
    """Entry point for the command-line utility."""
    # Import here to avoid circular dependency
    from .__main__ import main as _main
    return _main()


def fromProductionRequestYAMLToCWL(
    yaml_path: Path, production_name: str | None = None
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """Convert an LHCb Production Request YAML file into a CWL Workflow.

    This function routes to the appropriate converter based on production type.

    :param yaml_path: Path to the production request YAML file
    :param production_name: Name of the production to convert (if multiple in file)
    :return: Tuple of (CWL Workflow, CWL inputs dict, production metadata dict)
    """
    # Load YAML to determine production type
    with open(yaml_path, "r") as f:
        productions_data = yaml.safe_load(f)

    # Handle multiple productions in one file
    if not isinstance(productions_data, list):
        productions_data = [productions_data]

    # Find the requested production
    production_dict = None
    if production_name:
        for prod in productions_data:
            if prod.get("name") == production_name:
                production_dict = prod
                break
        if not production_dict:
            raise ValueError(f"Production '{production_name}' not found in {yaml_path}")
    else:
        if len(productions_data) > 1:
            names = [p.get("name") for p in productions_data]
            raise ValueError(f"Multiple productions found, please specify one: {names}")
        production_dict = productions_data[0]

    # Route based on production type
    production_type = production_dict.get("type")

    if production_type == "Simulation":
        return fromSimulationToCWL(yaml_path, production_name)
    elif production_type == "AnalysisProduction":
        return fromAnalysisToCWL(yaml_path, production_name)
    else:
        raise ValueError(
            f"Unknown production type: {production_type}. "
            f"Supported types: Simulation, AnalysisProduction"
        )


__all__ = [
    "fromProductionRequestYAMLToCWL",
    "main",
]

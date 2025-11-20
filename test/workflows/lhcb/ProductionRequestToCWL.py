###############################################################################
# (c) Copyright 2025 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""Converter to transform LHCb Production Request YAML files into CWL workflows.

This converter takes production request YAML files (e.g., simulation.yaml) and converts them
directly into CWL format, generating the necessary prodConf JSON files for each step.

The converter:
- Parses SimulationProduction YAML files
- Generates prodConf JSON configuration for each step
- Creates CWL workflow with CommandLineTool for each step
- Uses lb-prod-run as the base command for running applications
"""

import json
from pathlib import Path
from typing import Any

import yaml
from cwl_utils.parser.cwl_v1_2 import (
    CommandInputParameter,
    CommandLineBinding,
    CommandLineTool,
    CommandOutputBinding,
    CommandOutputParameter,
    Dirent,
    InitialWorkDirRequirement,
    MultipleInputFeatureRequirement,
    ResourceRequirement,
    StepInputExpressionRequirement,
    Workflow,
    WorkflowInputParameter,
    WorkflowOutputParameter,
    WorkflowStep,
    WorkflowStepInput,
    WorkflowStepOutput,
)


# Constants
POOL_XML = "pool-xml-catalog"
POOL_XML_OUT = f"{POOL_XML}-out"
RUN_NUMBER = "run-number"
FIRST_EVENT_NUMBER = "first-event-number"
NUMBER_OF_EVENTS = "number-of-events"


def fromProductionRequestYAMLToCWL(
    yaml_path: Path, production_name: str | None = None, event_type: str | None = None
) -> tuple[Workflow, dict[str, Any], dict[str, Any]]:
    """
    Convert an LHCb Production Request YAML file into a CWL Workflow.

    :param yaml_path: Path to the production request YAML file
    :param production_name: Name of the production to convert (if multiple in file)
    :param event_type: Event type ID to use for simulation
    :return: Tuple of (CWL Workflow, CWL inputs dict, production metadata dict)
    """
    # Load and parse YAML
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

    # Validate it's a simulation production
    if production_dict.get("type") != "Simulation":
        raise ValueError(f"Only Simulation productions are currently supported, got {production_dict.get('type')}")

    # Handle event type selection
    event_types = production_dict.get("event_types", [])
    if not event_types:
        raise ValueError("No event types found in production")

    selected_event_type = None
    if event_type:
        for et in event_types:
            if et.get("id") == event_type:
                selected_event_type = et
                break
        if not selected_event_type:
            available = [et.get("id") for et in event_types]
            raise ValueError(f"Event type '{event_type}' not found. Available: {available}")
    else:
        if len(event_types) > 1:
            available = [et.get("id") for et in event_types]
            raise ValueError(f"Multiple event types found, please specify one: {available}")
        selected_event_type = event_types[0]

    # Build the CWL workflow
    return _buildCWLWorkflow(production_dict, selected_event_type)


def _buildCWLWorkflow(
    production: dict[str, Any], event_type: dict[str, Any]
) -> tuple[Workflow, dict[str, Any], dict[str, Any]]:
    """Build a CWL workflow from a production dictionary."""

    production_name = production.get("name", "unknown_production")
    steps = production.get("steps", [])

    # Define workflow inputs
    workflow_inputs = _getWorkflowInputs(production, event_type)

    # Define workflow outputs
    workflow_outputs = _getWorkflowOutputs(steps)

    # Build CWL steps
    cwl_steps = []
    step_names = []  # Track step names for linking
    for step_index, step in enumerate(steps):
        step_name = _sanitizeStepName(step.get("name", f"step_{step_index}"))
        step_names.append(step_name)
        cwl_step = _buildCWLStep(production, step, step_index, workflow_inputs, step_names)
        cwl_steps.append(cwl_step)

    # Create resource requirements
    resource_requirement = ResourceRequirement(
        coresMin=1,
        coresMax=1,  # Can be enhanced based on multicore settings
    )

    # Build documentation with embedded metadata
    prod_metadata = {
        "production-name": production_name,
        "event-type": event_type.get("id"),
        "mc-config-version": production.get("mc_config_version"),
        "sim-condition": production.get("sim_condition"),
    }

    doc_lines = [
        f"LHCb Production Workflow: {production_name}",
        "",
        "Metadata:",
        f"  Event Type: {event_type.get('id')}",
        f"  MC Config Version: {production.get('mc_config_version')}",
        f"  Simulation Condition: {production.get('sim_condition')}",
        "",
        f"This workflow contains {len(cwl_steps)} processing steps.",
        "",
        "Generated by ProductionRequestToCWL converter",
    ]

    # Create the workflow with embedded metadata
    cwl_workflow = Workflow(
        cwlVersion="v1.2",
        label=production_name,
        doc="\n".join(doc_lines),
        steps=cwl_steps,
        requirements=[
            MultipleInputFeatureRequirement(),
            StepInputExpressionRequirement(),
            resource_requirement,
        ],
        inputs=list(workflow_inputs.values()),
        outputs=workflow_outputs,
    )

    # Note: cwl_inputs and prod_metadata are now optional
    # The workflow is self-contained with defaults and embedded metadata
    # These are returned for backward compatibility and optional use
    cwl_inputs = _getWorkflowStaticInputs(production, event_type)

    return cwl_workflow, cwl_inputs, prod_metadata


def _getWorkflowInputs(production: dict[str, Any], event_type: dict[str, Any]) -> dict[str, WorkflowInputParameter]:
    """Define the workflow-level inputs with default values."""
    workflow_inputs = {}

    # For Gauss (first step in simulation), we need specific inputs
    steps = production.get("steps", [])
    is_gauss = False
    if steps:
        first_app = steps[0].get("application", {})
        app_name = first_app.get("name", "") if isinstance(first_app, dict) else first_app.split("/")[0]
        is_gauss = app_name.lower() == "gauss"

    # Production identification inputs
    workflow_inputs["production-id"] = WorkflowInputParameter(
        type_="int",
        id="production-id",
        default=12345,
        doc="Production ID"
    )
    workflow_inputs["prod-job-id"] = WorkflowInputParameter(
        type_="int",
        id="prod-job-id",
        default=6789,
        doc="Production Job ID"
    )

    if is_gauss:
        # Gauss-specific inputs with default values
        workflow_inputs[RUN_NUMBER] = WorkflowInputParameter(
            type_="int",
            id=RUN_NUMBER,
            default=1,
            doc="Run number for simulation"
        )
        workflow_inputs[FIRST_EVENT_NUMBER] = WorkflowInputParameter(
            type_="int",
            id=FIRST_EVENT_NUMBER,
            default=1,
            doc="First event number in the run"
        )
        workflow_inputs[NUMBER_OF_EVENTS] = WorkflowInputParameter(
            type_="int",
            id=NUMBER_OF_EVENTS,
            default=event_type.get("num_test_events", 10),
            doc="Number of events to generate"
        )
        workflow_inputs[POOL_XML] = WorkflowInputParameter(
            type_="string",
            id=POOL_XML,
            default="pool_xml_catalog.xml",
            doc="Pool XML catalog file name"
        )
        workflow_inputs["histogram"] = WorkflowInputParameter(
            type_="boolean",
            id="histogram",
            default=False,
            doc="Enable histogram output"
        )
    else:
        # For non-Gauss starts, we need input data
        workflow_inputs["input-data"] = WorkflowInputParameter(
            type_="File",
            id="input-data",
            doc="Input data files from previous step"
        )
        workflow_inputs[POOL_XML] = WorkflowInputParameter(
            type_="File",
            id=POOL_XML,
            doc="Pool XML catalog file"
        )
        workflow_inputs[NUMBER_OF_EVENTS] = WorkflowInputParameter(
            type_="int",
            id=NUMBER_OF_EVENTS,
            default=event_type.get("num_test_events", 10),
            doc="Number of events to process"
        )

    # Common dynamic inputs with defaults
    workflow_inputs["output-prefix"] = WorkflowInputParameter(
        type_="string",
        id="output-prefix",
        default=event_type.get("id", "output"),
        doc="Prefix for output file names"
    )

    return workflow_inputs


def _getWorkflowOutputs(steps: list[dict[str, Any]]) -> list[WorkflowOutputParameter]:
    """Define the workflow-level outputs."""
    workflow_outputs = []

    # Collect outputs from the last step
    if steps:
        last_step = steps[-1]
        step_name = _sanitizeStepName(last_step.get("name", f"step_{len(steps) - 1}"))

        # Output data files
        workflow_outputs.append(
            WorkflowOutputParameter(
                type_="File[]",
                id="output-data",
                label="Output Data",
                outputSource=f"{step_name}/output-data",
            )
        )

        # Other outputs (logs, summary files, etc.)
        workflow_outputs.append(
            WorkflowOutputParameter(
                type_="File[]",
                id="others",
                label="Other outputs (logs, summaries)",
                outputSource=f"{step_name}/others",
            )
        )

        # Pool XML catalog
        workflow_outputs.append(
            WorkflowOutputParameter(
                type_="File",
                id=POOL_XML_OUT,
                label="Pool XML Catalog",
                outputSource=f"{step_name}/{POOL_XML_OUT}",
            )
        )

    return workflow_outputs


def _buildCWLStep(
    production: dict[str, Any],
    step: dict[str, Any],
    step_index: int,
    workflow_inputs: dict[str, WorkflowInputParameter],
    step_names: list[str],
) -> WorkflowStep:
    """Build a CWL WorkflowStep for a single production step."""

    step_name = step_names[step_index]

    # Generate prodConf configuration
    prod_conf = _generateProdConf(production, step, step_index)

    # Build command line tool
    command_tool = _buildCommandLineTool(step, step_index, prod_conf, workflow_inputs)

    # Build step inputs
    step_inputs = _buildStepInputs(step, step_index, workflow_inputs, step_names)

    # Build step outputs
    step_outputs = _buildStepOutputs(step)

    return WorkflowStep(
        id=step_name,
        run=command_tool,
        in_=step_inputs,
        out=step_outputs,
    )


def _generateProdConf(production: dict[str, Any], step: dict[str, Any], step_index: int) -> dict[str, Any]:
    """Generate the prodConf JSON configuration for a step (similar to RunApplication.py)."""

    # Parse application info
    application = step.get("application", {})
    if isinstance(application, str):
        # Format: "AppName/version"
        parts = application.split("/")
        app_name = parts[0]
        app_version = parts[1] if len(parts) > 1 else "unknown"
        binary_tag = None
        nightly = None
    else:
        app_name = application.get("name", "unknown")
        app_version = application.get("version", "unknown")
        binary_tag = application.get("binary_tag")
        nightly = application.get("nightly")

    # Parse data packages
    data_pkgs = []
    for pkg in step.get("data_pkgs", []):
        if isinstance(pkg, str):
            data_pkgs.append(pkg)
        else:
            data_pkgs.append(f"{pkg.get('name')}.{pkg.get('version')}")

    # Build prodConf structure
    prod_conf = {
        "spec_version": 1,
        "application": {
            "name": app_name,
            "version": app_version,
            "number_of_processors": 1,  # Will be overridden by dynamic input
            "data_pkgs": data_pkgs,
        },
        "options": {},
        "db_tags": {},
        "input": {
            "files": [],  # Will be provided via command line
            "xml_file_catalog": "pool_xml_catalog.xml",  # Default name
        },
        "output": {
            "prefix": "",  # Will be provided via command line
            "types": [],
        },
    }

    # Add binary tag if specified
    if binary_tag:
        prod_conf["application"]["binary_tag"] = binary_tag

    # Add nightly if specified
    if nightly:
        prod_conf["application"]["nightly"] = nightly

    # Build options configuration
    options = step.get("options", [])
    options_format = step.get("options_format")
    processing_pass = step.get("processing_pass")

    if isinstance(options, dict):
        # LbExec or other structured format
        prod_conf["options"] = options
    elif isinstance(options, list):
        # Legacy format - list of option files
        event_type_id = production.get("event_types", [{}])[0].get("id", "")
        # Replace @{eventType} placeholder
        processed_options = [opt.replace("@{eventType}", event_type_id) for opt in options]
        prod_conf["options"]["files"] = processed_options
        if processing_pass:
            prod_conf["options"]["processing_pass"] = processing_pass
        if options_format:
            prod_conf["options"]["format"] = options_format

    # DB Tags
    dbtags = step.get("dbtags", {})
    if dbtags:
        if dbtags.get("DDDB"):
            prod_conf["db_tags"]["dddb_tag"] = dbtags["DDDB"]
        if dbtags.get("CondDB"):
            prod_conf["db_tags"]["conddb_tag"] = dbtags["CondDB"]
        if dbtags.get("DQTag"):
            prod_conf["db_tags"]["dq_tag"] = dbtags["DQTag"]

    # Output types
    output_types = []
    for output in step.get("output", []):
        output_type = output.get("type")
        if output_type:
            output_types.append(output_type)
    prod_conf["output"]["types"] = output_types

    # Input configuration (number of events, etc.)
    # These will be provided dynamically for Gauss
    prod_conf["input"]["n_of_events"] = -1  # Will be set via command line for Gauss

    return prod_conf


def _buildCommandLineTool(
    step: dict[str, Any],
    step_index: int,
    prod_conf: dict[str, Any],
    workflow_inputs: dict[str, WorkflowInputParameter],
) -> CommandLineTool:
    """Build a CommandLineTool for a step using command-line wrapper."""

    # Determine if this is a Gauss step
    application = step.get("application", {})
    if isinstance(application, str):
        app_name = application.split("/")[0]
    else:
        app_name = application.get("name", "unknown")
    is_gauss = app_name.lower() == "gauss"

    # Step number is 1-indexed
    step_number = step_index + 1

    # Build input parameters with command-line bindings
    input_parameters = []

    # Production identification inputs (needed for filename generation)
    input_parameters.append(
        CommandInputParameter(
            id="production-id",
            type_="int",
            inputBinding=CommandLineBinding(prefix="--production-id"),
        )
    )
    input_parameters.append(
        CommandInputParameter(
            id="prod-job-id",
            type_="int",
            inputBinding=CommandLineBinding(prefix="--prod-job-id"),
        )
    )

    # Add inputs based on what's in workflow_inputs
    for input_id in workflow_inputs.keys():
        if input_id in ["production-id", "prod-job-id"]:
            # Already added above
            continue
        elif input_id == "input-data":
            if step_index > 0:
                # For steps after Gauss, input data comes from previous step (PFN paths)
                input_parameters.append(
                    CommandInputParameter(
                        id="input-data",
                        type_="File[]",
                        inputBinding=CommandLineBinding(prefix="--pfn-paths"),
                    )
                )
            else:
                # First step uses LFN paths
                input_parameters.append(
                    CommandInputParameter(
                        id="input-data",
                        type_="File",
                        inputBinding=CommandLineBinding(prefix="--lfn-paths"),
                    )
                )
        elif input_id == POOL_XML:
            input_parameters.append(
                CommandInputParameter(
                    id=POOL_XML,
                    type_="File" if step_index > 0 else "string",
                    inputBinding=CommandLineBinding(prefix="--pool-xml-catalog"),
                )
            )
        elif input_id == RUN_NUMBER:
            input_parameters.append(
                CommandInputParameter(
                    id=RUN_NUMBER,
                    type_="int?",
                    inputBinding=CommandLineBinding(prefix="--run-number"),
                )
            )
        elif input_id == FIRST_EVENT_NUMBER and is_gauss:
            # Only add first-event-number for Gauss steps
            input_parameters.append(
                CommandInputParameter(
                    id=FIRST_EVENT_NUMBER,
                    type_="int?",
                    inputBinding=CommandLineBinding(prefix="--first-event-number"),
                )
            )
        elif input_id == NUMBER_OF_EVENTS:
            input_parameters.append(
                CommandInputParameter(
                    id=NUMBER_OF_EVENTS,
                    type_="int",
                    inputBinding=CommandLineBinding(prefix="--number-of-events"),
                )
            )
        elif input_id == "output-prefix":
            input_parameters.append(
                CommandInputParameter(
                    id="output-prefix",
                    type_="string",
                    inputBinding=CommandLineBinding(prefix="--output-prefix"),
                )
            )
        elif input_id == "histogram" and is_gauss:
            # Only add histogram for Gauss steps
            input_parameters.append(
                CommandInputParameter(
                    id="histogram",
                    type_="boolean",
                    inputBinding=CommandLineBinding(prefix="--histogram"),
                )
            )

    # Create readable multi-line JSON string for base configuration
    # Use a LiteralScalarString to preserve formatting in YAML output
    from ruamel.yaml.scalarstring import LiteralScalarString
    config_json = LiteralScalarString(json.dumps(prod_conf, indent=2))

    # Use InitialWorkDirRequirement to write the base config with dynamic filename
    initial_prod_conf = f"initialProdConf_{step_number}.json"
    requirements = [
        InitialWorkDirRequirement(
            listing=[
                Dirent(
                    entryname=initial_prod_conf,
                    entry=config_json,
                )
            ]
        ),
        # Add ResourceRequirement for cores (default to 1)
        ResourceRequirement(
            coresMin=1,
            coresMax=1,
        ),
        # Need StepInputExpressionRequirement for the filename expression
        StepInputExpressionRequirement(),
    ]

    # Build output parameters
    output_parameters = _buildOutputParameters(step)

    # Create the CommandLineTool using the wrapper
    return CommandLineTool(
        inputs=input_parameters,
        outputs=output_parameters,
        baseCommand=["dirac-run-lbprodrun-app"],
        arguments=[initial_prod_conf, "--step-id", str(step_number)],
        requirements=requirements,
    )


def _buildOutputParameters(step: dict[str, Any]) -> list[CommandOutputParameter]:
    """Build output parameters for a step."""
    output_parameters = []

    # Get output types from step
    output_types = step.get("output", [])

    # Main output data
    output_globs = []
    for output in output_types:
        output_type = output.get("type")
        if output_type:
            output_globs.append(f"*.{output_type.lower()}")

    if output_globs:
        output_parameters.append(
            CommandOutputParameter(
                id="output-data",
                type_="File[]",
                outputBinding=CommandOutputBinding(glob=output_globs),
            )
        )

    # Other outputs (logs, summaries, prodConf files)
    application = step.get("application", {})
    if isinstance(application, str):
        app_name = application.split("/")[0]
    else:
        app_name = application.get("name", "app")

    output_parameters.append(
        CommandOutputParameter(
            id="others",
            type_="File[]",
            outputBinding=CommandOutputBinding(
                glob=[
                    "prodConf*.json",
                    "prodConf*.py",
                    "summary*.xml",
                    "prmon*",
                    f"{app_name.replace('/', '').replace(' ', '')}*.log",
                ]
            ),
        )
    )

    # Pool XML catalog output
    output_parameters.append(
        CommandOutputParameter(
            id=POOL_XML_OUT,
            type_="File",
            outputBinding=CommandOutputBinding(glob="pool_xml_catalog.xml"),
        )
    )

    return output_parameters


def _buildStepInputs(
    step: dict[str, Any],
    step_index: int,
    workflow_inputs: dict[str, WorkflowInputParameter],
    step_names: list[str],
) -> list[WorkflowStepInput]:
    """Build step inputs, linking to workflow inputs or previous steps."""
    step_inputs = []

    # Determine if this is a Gauss step
    application = step.get("application", {})
    if isinstance(application, str):
        app_name = application.split("/")[0]
    else:
        app_name = application.get("name", "unknown")
    is_gauss = app_name.lower() == "gauss"

    # Always add production-id and prod-job-id
    step_inputs.append(
        WorkflowStepInput(
            id="production-id",
            source="production-id",
        )
    )
    step_inputs.append(
        WorkflowStepInput(
            id="prod-job-id",
            source="prod-job-id",
        )
    )

    for input_id, wf_input in workflow_inputs.items():
        # Skip production IDs as they're already added
        if input_id in ["production-id", "prod-job-id"]:
            continue

        # Skip first-event-number for non-Gauss steps
        if input_id == FIRST_EVENT_NUMBER and not is_gauss:
            continue

        # Skip histogram for non-Gauss steps
        if input_id == "histogram" and not is_gauss:
            continue

        source = wf_input.id
        value_from = None

        if input_id == "output-prefix":
            # Add step index to output prefix (1-indexed)
            value_from = f'$(inputs["output-prefix"])_{step_index + 1}'
        elif input_id == "input-data":
            # Link to previous step's output if not first step
            if step_index > 0:
                prev_step_name = step_names[step_index - 1]
                source = f"{prev_step_name}/output-data"
        elif input_id == POOL_XML:
            # Link to previous step's pool XML if not first step
            if step_index > 0:
                prev_step_name = step_names[step_index - 1]
                source = f"{prev_step_name}/{POOL_XML_OUT}"

        step_inputs.append(
            WorkflowStepInput(
                id=input_id,
                source=source,
                valueFrom=value_from,
            )
        )

    return step_inputs


def _buildStepOutputs(step: dict[str, Any]) -> list[WorkflowStepOutput]:
    """Build step outputs."""
    return [
        WorkflowStepOutput(id="output-data"),
        WorkflowStepOutput(id="others"),
        WorkflowStepOutput(id=POOL_XML_OUT),
    ]


def _getWorkflowStaticInputs(production: dict[str, Any], event_type: dict[str, Any]) -> dict[str, Any]:
    """Get static input values for CWL execution."""
    static_inputs = {}

    # Production identification inputs (defaults for testing)
    static_inputs["production-id"] = 12345
    static_inputs["prod-job-id"] = 6789

    # Check if first step is Gauss
    steps = production.get("steps", [])
    is_gauss = False
    if steps:
        first_app = steps[0].get("application", {})
        app_name = first_app.get("name", "") if isinstance(first_app, dict) else first_app.split("/")[0]
        is_gauss = app_name.lower() == "gauss"

    if is_gauss:
        # Gauss-specific static inputs
        static_inputs[RUN_NUMBER] = 1  # Default run number
        static_inputs[FIRST_EVENT_NUMBER] = 1  # Default first event
        static_inputs[NUMBER_OF_EVENTS] = event_type.get("num_test_events", 10)
        static_inputs[POOL_XML] = "pool_xml_catalog.xml"  # String for Gauss
        static_inputs["histogram"] = False
    else:
        # For non-Gauss, would need actual input files
        static_inputs[POOL_XML] = {"class": "File", "path": "pool_xml_catalog.xml"}
        static_inputs[NUMBER_OF_EVENTS] = event_type.get("num_test_events", 10)

    # Common dynamic inputs with defaults
    static_inputs["output-prefix"] = event_type.get("id", "output")

    return static_inputs


def _sanitizeStepName(name: str) -> str:
    """Sanitize step name to be CWL-compatible (no spaces, special chars)."""
    # Replace spaces and special characters with underscores
    import re

    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    # Ensure it starts with a letter or underscore
    if sanitized and not sanitized[0].isalpha() and sanitized[0] != "_":
        sanitized = "_" + sanitized
    return sanitized or "step"

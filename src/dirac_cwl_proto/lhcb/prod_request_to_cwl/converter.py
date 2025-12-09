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
    CommandInputEnumSchema,
    CommandInputParameter,
    CommandLineBinding,
    CommandLineTool,
    CommandOutputBinding,
    CommandOutputParameter,
    Dirent,
    InitialWorkDirRequirement,
    InputEnumSchema,
    MultipleInputFeatureRequirement,
    ResourceRequirement,
    StepInputExpressionRequirement,
    InlineJavascriptRequirement,
    Workflow,
    WorkflowInputParameter,
    WorkflowOutputParameter,
    WorkflowStep,
    WorkflowStepInput,
    WorkflowStepOutput,
)
from ruamel.yaml.scalarstring import LiteralScalarString


# Constants
POOL_XML = "pool-xml-catalog"
POOL_XML_OUT = f"{POOL_XML}-out"
EVENT_TYPE = "event-type"
RUN_NUMBER = "run-number"
FIRST_EVENT_NUMBER = "first-event-number"
NUMBER_OF_EVENTS = "number-of-events"


def fromProductionRequestYAMLToCWL(
    yaml_path: Path, production_name: str | None = None
) -> tuple[Workflow, dict[str, Any], dict[str, Any]]:
    """
    Convert an LHCb Production Request YAML file into a CWL Workflow.

    :param yaml_path: Path to the production request YAML file
    :param production_name: Name of the production to convert (if multiple in file)
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
    if production_dict.get("type") not in ["Simulation"]:
        raise ValueError(f"Only Simulation productions are currently supported, got {production_dict.get('type')}")

    # Handle event type selection
    event_types = production_dict.get("event_types", [])
    if not event_types:
        raise ValueError("No event types found in production")

    # Build the CWL workflow
    return _buildCWLWorkflow(production_dict)


def _buildCWLWorkflow(
    production: dict[str, Any]
) -> tuple[Workflow, dict[str, Any], dict[str, Any]]:
    """Build a CWL workflow from a production dictionary.

    The workflow is structured with transformations as sub-workflows:
    - Main workflow has one step per transformation
    - Each transformation is a sub-workflow containing the actual processing steps
    """

    production_name = production.get("name", "unknown_production")
    steps = production.get("steps", [])
    submission_info = production.get("submission_info", {})
    transforms = submission_info.get("transforms", [])

    # Define workflow inputs
    workflow_inputs = _getWorkflowInputs(production)

    # If no transforms defined, fall back to single transformation containing all steps
    if not transforms:
        transforms = [{
            "steps": list(range(1, len(steps) + 1)),  # 1-indexed
            "type": "Processing",
        }]

    # Build transformation sub-workflows
    transformation_workflows = []
    transformation_names = []
    for transform_index, transform in enumerate(transforms):
        transform_name = _sanitizeStepName(f"transformation_{transform_index + 1}")
        transformation_names.append(transform_name)

        # Build the sub-workflow for this transformation
        transformation_workflow = _buildTransformationWorkflow(
            production, transform, transform_index, workflow_inputs
        )
        transformation_workflows.append(transformation_workflow)

    # Build main workflow steps (one per transformation)
    main_workflow_steps = []
    for transform_index, (transform_name, transform_workflow, transform) in enumerate(
        zip(transformation_names, transformation_workflows, transforms)
    ):
        main_step = _buildMainWorkflowStep(
            transform_name, transform_workflow, transform, transform_index,
            workflow_inputs, transformation_names
        )
        main_workflow_steps.append(main_step)

    # Define workflow outputs from the last transformation
    workflow_outputs = _getMainWorkflowOutputs(steps, transforms, transformation_names)

    # Create resource requirements
    resource_requirement = ResourceRequirement(
        coresMin=1,
        coresMax=1,  # Can be enhanced based on multicore settings
    )

    # Build documentation with embedded metadata
    prod_metadata = {
        "production-name": production_name,
        "mc-config-version": production.get("mc_config_version"),
        "sim-condition": production.get("sim_condition"),
    }

    doc_lines = [
        f"LHCb Production Workflow: {production_name}",
        "",
        "Metadata:",
        f"  Event Types:\n    - {'\n    - '.join(f'{evttype['id']}: {evttype['num_events']} events' for evttype in production.get('event_types'))}",
        f"  MC Config Version: {production.get('mc_config_version')}",
        f"  Simulation Condition: {production.get('sim_condition')}",
        "",
        f"This workflow contains {len(transforms)} transformation(s).",
        "",
        "Generated by ProductionRequestToCWL converter",
    ]

    # Create the workflow with embedded metadata
    # Create a readable workflow ID from the production name
    workflow_id = _sanitizeStepName(production_name)

    cwl_workflow = Workflow(
        id=workflow_id,
        cwlVersion="v1.2",
        label=production_name,
        doc=LiteralScalarString("\n".join(doc_lines)),
        steps=main_workflow_steps,
        requirements=[
            MultipleInputFeatureRequirement(),
            StepInputExpressionRequirement(),
            InlineJavascriptRequirement(),
            resource_requirement,
        ],
        inputs=list(workflow_inputs.values()),
        outputs=workflow_outputs,
    )

    # Note: cwl_inputs and prod_metadata are now optional
    # The workflow is self-contained with defaults and embedded metadata
    # These are returned for backward compatibility and optional use
    cwl_inputs = _getWorkflowStaticInputs(production)

    return cwl_workflow, cwl_inputs, prod_metadata


def _getWorkflowInputs(production: dict[str, Any]) -> dict[str, WorkflowInputParameter]:
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
        # Create enum type for event-type to restrict to valid event types
        event_types = production.get("event_types", [])
        event_type_ids = [str(evt["id"]) for evt in event_types]

        if event_type_ids:
            # Create an enum schema with the valid event type IDs
            event_type_enum = InputEnumSchema(
                type_="enum",
                symbols=event_type_ids,
                name="EventTypeEnum",
            )
            workflow_inputs[EVENT_TYPE] = WorkflowInputParameter(
                type_=event_type_enum,
                id=EVENT_TYPE,
                doc="Event type to be generated"
            )
        else:
            # Fallback to int if no event types are specified
            workflow_inputs[EVENT_TYPE] = WorkflowInputParameter(
                type_="int",
                id=EVENT_TYPE,
                doc="Event type to be generated"
            )
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
            default=10,
            doc="Number of events to generate"
        )
        workflow_inputs[POOL_XML] = WorkflowInputParameter(
            type_="File?",
            id=POOL_XML,
            doc="Pool XML catalog file (optional)"
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
            default=10,
            doc="Number of events to process"
        )

    return workflow_inputs


def _getWorkflowOutputs(steps: list[dict[str, Any]]) -> list[WorkflowOutputParameter]:
    """Define the workflow-level outputs based on output file visibility flags."""
    workflow_outputs = []
    allOutputSources = []
    otherOutputSources = []

    # Collect outputs from steps that have visible output files
    for step_index, step in enumerate(steps):
        # Check if any output file in this step is marked as visible
        visible_outputs = [out for out in step.get("output", []) if out.get("visible", False)]

        if visible_outputs:
            step_name = _sanitizeStepName(step.get("name", f"step_{step_index}"))
            allOutputSources.append(f"{step_name}/output-data")

    # Collect "others" outputs (logs, summaries) from ALL steps for log storage
    for step_index, step in enumerate(steps):
        step_name = _sanitizeStepName(step.get("name", f"step_{step_index}"))
        otherOutputSources.append(f"{step_name}/others")


    # Output data files for this visible step
    workflow_outputs.append(
        WorkflowOutputParameter(
            type_="File[]",
            id="output-data",
            label="Output Data",
            outputSource=allOutputSources,
            linkMerge="merge_flattened",
        )
    )

    # Other outputs (logs, summaries) from all steps
    # Generally uploaded to some LogSE or the job sandbox
    workflow_outputs.append(
        WorkflowOutputParameter(
            type_="File[]",
            id="others",
            label="Logs and summaries",
            outputSource=otherOutputSources,
            linkMerge="merge_flattened",
        )
    )

    # Always add pool XML catalog from the last step
    if steps:
        last_step = steps[-1]
        last_step_name = _sanitizeStepName(last_step.get("name", f"step_{len(steps) - 1}"))

        # Pool XML catalog
        workflow_outputs.append(
            WorkflowOutputParameter(
                type_="File",
                id=POOL_XML_OUT,
                label="Pool XML Catalog",
                outputSource=f"{last_step_name}/{POOL_XML_OUT}",
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
    command_tool = _buildCommandLineTool(step, step_index, prod_conf, workflow_inputs, step_name)

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
            # "number_of_processors": 1,  # Will be overridden by dynamic input
            "data_pkgs": data_pkgs,
        },
        "options": {},
        "db_tags": {},
        "input": {},
        "output": {},
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
        # Ensure @{eventType} placeholder is present
        # lb-prod-run will substitute it at runtime
        if not [opt for opt in options if "@{eventType}" in opt] and app_name.lower() == "gauss":
            raise ValueError("For Gauss, at least one option file path must contain the '@{eventType}' placeholder.")
        prod_conf["options"]["files"] = options
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
    step_name: str,
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

    # pool_xml_catalog filename
    pool_xml_filename = "pool_xml_catalog.xml"

    # Build input parameters with command-line bindings
    input_parameters = []
    input_parameters.append(
        CommandInputParameter(
            id="output-prefix",
            type_="string",
            inputBinding=CommandLineBinding(prefix="--output-prefix"),
        )
    )

    # For non-first steps, always add input-data parameter (receives output from previous step)
    # We'll create a manifest file via InitialWorkDirRequirement to avoid command-line length limits
    if step_index > 0:
        input_parameters.append(
            CommandInputParameter(
                id="input-data",
                type_="File[]",
                # No inputBinding here - we'll handle it via InitialWorkDirRequirement
            )
        )

    # Add inputs based on what's in workflow_inputs
    for input_id in workflow_inputs.keys():
        if input_id in ["production-id", "prod-job-id", "output-prefix"]:
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
            # Pool XML catalog is copied via InitialWorkDirRequirement, not passed as CLI arg
            # Only add as input parameter (no inputBinding) so it can be used in InitialWorkDirRequirement
            input_parameters.append(
                CommandInputParameter(
                    id=POOL_XML,
                    type_="File?" if step_index == 0 else "File",
                    # No inputBinding - we copy it via InitialWorkDirRequirement instead
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
        elif input_id == EVENT_TYPE and is_gauss:
            # Get the workflow input to check if it's an enum type
            wf_input = workflow_inputs[EVENT_TYPE]
            if isinstance(wf_input.type_, InputEnumSchema):
                # Create a CommandInputEnumSchema with the same symbols
                cmd_enum = CommandInputEnumSchema(
                    type_="enum",
                    symbols=wf_input.type_.symbols,
                    name=f"{step_name}_EventTypeEnum",
                    inputBinding=CommandLineBinding(prefix="--event-type"),
                )
                input_parameters.append(
                    CommandInputParameter(
                        id=EVENT_TYPE,
                        type_=cmd_enum,
                    )
                )
            else:
                # Fallback to string if not an enum
                input_parameters.append(
                    CommandInputParameter(
                        id=EVENT_TYPE,
                        type_="string",
                        inputBinding=CommandLineBinding(prefix="--event-type"),
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
    config_json = LiteralScalarString(json.dumps(prod_conf, indent=2))

    # Use InitialWorkDirRequirement to write the base config with dynamic filename
    initial_prod_conf = f"initialProdConf_{step_number}.json"

    # Build the listing for InitialWorkDirRequirement
    initial_workdir_listing = [
        Dirent(
            entryname=initial_prod_conf,
            entry=config_json,
        )
    ]

    # For all steps, copy pool_xml_catalog if provided
    # For the first step, this input is optional (File?) - if provided, copy it; otherwise Gauss creates it
    # For subsequent steps, this will be a required File from the previous step that should be copied
    # We always add the copy directive - if the input is null/missing, the file won't be created
    initial_workdir_listing.append(
        Dirent(
            entryname=pool_xml_filename,
            entry=f"$(inputs['{POOL_XML}'])",
        )
    )

    # For non-first steps, create an input files manifest to avoid command-line length limits
    # This manifest will contain one file path per line
    if step_index > 0:
        input_files_manifest = f"inputFiles_{step_number}.txt"
        # Use a simpler JavaScript expression that maps over the files and joins with newlines
        input_files_expr = "$(inputs['input-data'].map(function(f) { return f.path; }).join('\\n'))"
        initial_workdir_listing.append(
            Dirent(
                entryname=input_files_manifest,
                entry=input_files_expr,
            )
        )

    requirements = [
        InitialWorkDirRequirement(listing=initial_workdir_listing),
        # Add ResourceRequirement for cores (default to 1)
        ResourceRequirement(
            coresMin=1,
            coresMax=1,
        ),
        # Need StepInputExpressionRequirement for the filename expression
        StepInputExpressionRequirement(),
        # Need InlineJavascriptRequirement for JavaScript expressions in InitialWorkDirRequirement
        InlineJavascriptRequirement(),
    ]

    # Build output parameters
    output_parameters = _buildOutputParameters(step)

    # Create the CommandLineTool using the wrapper
    # Use step name as the tool ID for readability
    tool_id = f"{step_name}_tool"

    # Build arguments - add input files manifest for non-first steps
    arguments = [initial_prod_conf]
    if step_index > 0:
        input_files_manifest = f"inputFiles_{step_number}.txt"
        arguments.extend(["--input-files", input_files_manifest])

    return CommandLineTool(
        id=tool_id,
        inputs=input_parameters,
        outputs=output_parameters,
        baseCommand=["dirac-run-lbprodrun-app"],
        arguments=arguments,
        requirements=requirements,
    )


def _make_case_insensitive_glob(extension: str) -> str:
    """
    Convert an extension to a case-insensitive glob pattern using character classes.

    For example: "allstreams.dst" -> "*.[aA][lL][lL][sS][tT][rR][eE][aA][mM][sS].[dD][sS][tT]"
    """
    result = "*."
    for char in extension:
        if char == '.':
            result += '.'
        elif char.isalpha():
            result += f"[{char.lower()}{char.upper()}]"
        else:
            result += char
    return result


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
            # Use case-insensitive glob pattern to handle any case variation
            output_globs.append(_make_case_insensitive_glob(output_type.lower()))

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

    # Add output-prefix computed from production-id and prod-job-id
    # Use multiple sources and valueFrom to compute the prefix
    step_inputs.append(
        WorkflowStepInput(
            id="output-prefix",
            source=["production-id", "prod-job-id"],
            valueFrom=f'$(self[0].toString().padStart(8, "0"))_$(self[1].toString().padStart(8, "0"))_{step_index + 1}',
        )
    )

    # For non-first steps, always add input-data from previous step
    if step_index > 0:
        prev_step_name = step_names[step_index - 1]
        step_inputs.append(
            WorkflowStepInput(
                id="input-data",
                source=f"{prev_step_name}/output-data",
            )
        )

    for input_id, wf_input in workflow_inputs.items():
        # Skip production IDs and output-prefix as they're already handled above
        if input_id in ["production-id", "prod-job-id", "output-prefix"]:
            continue

        # Skip input-data as it's already handled above for non-first steps
        if input_id == "input-data":
            continue

        # Skip first-event-number for non-Gauss steps
        if input_id == FIRST_EVENT_NUMBER and not is_gauss:
            continue

        # Skip event-type for non-Gauss steps
        if input_id == EVENT_TYPE and not is_gauss:
            continue

        # Skip histogram for non-Gauss steps
        if input_id == "histogram" and not is_gauss:
            continue

        source = wf_input.id
        value_from = None

        if input_id == POOL_XML:
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


def _getWorkflowStaticInputs(production: dict[str, Any]) -> dict[str, Any]:
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
        static_inputs[NUMBER_OF_EVENTS] = 10
        static_inputs[POOL_XML] = "pool_xml_catalog.xml"  # String for Gauss
        static_inputs["histogram"] = False
    else:
        # For non-Gauss, would need actual input files
        static_inputs[POOL_XML] = {"class": "File", "path": "pool_xml_catalog.xml"}
        static_inputs[NUMBER_OF_EVENTS] = 10

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


def _buildTransformationWorkflow(
    production: dict[str, Any],
    transform: dict[str, Any],
    transform_index: int,
    production_inputs: dict[str, WorkflowInputParameter],
) -> Workflow:
    """Build a CWL sub-workflow for a single transformation.

    A transformation contains one or more processing steps that run within one job.

    :param production: The full production dictionary
    :param transform: The transformation definition from submission_info
    :param transform_index: Index of this transformation (0-based)
    :param production_inputs: Input parameters from the production level
    :return: A Workflow representing this transformation
    """
    steps = production.get("steps", [])
    transform_step_indices = transform.get("steps", [])  # 1-indexed

    # Convert 1-indexed step numbers to 0-indexed
    step_indices = [idx - 1 for idx in transform_step_indices]

    # Get the actual steps for this transformation
    transform_steps = [steps[idx] for idx in step_indices]

    # Build transformation-level inputs (same as production inputs for now)
    transformation_inputs = production_inputs.copy()

    # Build CWL steps for this transformation
    cwl_steps = []
    step_names = []
    for local_step_index, global_step_index in enumerate(step_indices):
        step = steps[global_step_index]
        step_name = _sanitizeStepName(step.get("name", f"step_{global_step_index}"))
        step_names.append(step_name)

        # Build the step with local indexing within the transformation
        cwl_step = _buildCWLStep(
            production, step, local_step_index, transformation_inputs, step_names
        )
        cwl_steps.append(cwl_step)

    # Define transformation outputs
    transformation_outputs = _getTransformationOutputs(transform_steps, step_names)

    # Build transformation hints with submission_info metadata
    hints = _buildTransformationHints(transform)

    # Create the transformation sub-workflow
    transform_name = _sanitizeStepName(f"transformation_{transform_index + 1}")
    transform_type = transform.get("type", "Processing")

    doc_lines = [
        f"Transformation {transform_index + 1}: {transform_type}",
        f"Contains {len(transform_steps)} step(s)",
    ]

    transformation_workflow = Workflow(
        id=transform_name,
        cwlVersion="v1.2",
        label=f"Transformation {transform_index + 1}",
        doc=LiteralScalarString("\n".join(doc_lines)),
        steps=cwl_steps,
        requirements=[
            MultipleInputFeatureRequirement(),
            StepInputExpressionRequirement(),
            InlineJavascriptRequirement(),
        ],
        inputs=list(transformation_inputs.values()),
        outputs=transformation_outputs,
        hints=hints,
    )

    return transformation_workflow


def _buildTransformationHints(transform: dict[str, Any]) -> list[dict[str, Any]]:
    """Build CWL hints for a transformation from submission_info.

    This includes TransformationExecutionHooksHint with parameters like:
    - group_size
    - cpu
    - priority
    - output_mode
    - etc.

    :param transform: The transformation definition from submission_info
    :return: List of hint dictionaries
    """
    hints = []

    # Build TransformationExecutionHooksHint from submission_info
    hint = {"class": "dirac:TransformationExecutionHooks"}

    # Map submission_info fields to hint configuration
    config = {}

    # Direct mappings
    if "cpu" in transform:
        config["cpu"] = transform["cpu"]
    if "priority" in transform:
        config["priority"] = transform["priority"]
    if "output_mode" in transform:
        config["output_mode"] = transform["output_mode"]
    if "output_se" in transform:
        config["output_se"] = transform["output_se"]
    if "input_data_policy" in transform:
        config["input_data_policy"] = transform["input_data_policy"]
    if "input_plugin" in transform:
        config["input_plugin"] = transform["input_plugin"]
    if "remove_inputs_flags" in transform:
        config["remove_inputs_flags"] = transform["remove_inputs_flags"]
    if "events" in transform:
        config["events"] = transform["events"]

    # Group size needs special handling - map it to input names
    if "group_size" in transform:
        # For now, we'll use a generic input name
        # This could be enhanced to map to actual input names
        hint["group_size"] = {"input-data": transform["group_size"]}

    # Add configuration if we have any
    if config:
        hint["configuration"] = config

    # Add transformation type
    if "type" in transform:
        hint["hook_plugin"] = transform["type"]

    hints.append(hint)

    return hints


def _buildMainWorkflowStep(
    transform_name: str,
    transform_workflow: Workflow,
    transform: dict[str, Any],
    transform_index: int,
    workflow_inputs: dict[str, WorkflowInputParameter],
    transformation_names: list[str],
) -> WorkflowStep:
    """Build a step in the main workflow that references a transformation sub-workflow.

    :param transform_name: Name of this transformation
    :param transform_workflow: The transformation sub-workflow
    :param transform: The transformation definition from submission_info
    :param transform_index: Index of this transformation (0-based)
    :param workflow_inputs: Input parameters from the production level
    :param transformation_names: Names of all transformations (for linking)
    :return: A WorkflowStep for the main workflow
    """
    # Build step inputs
    step_inputs = []

    # If this is not the first transformation, link to previous transformation's outputs
    if transform_index > 0:
        prev_transform_name = transformation_names[transform_index - 1]

        # Link output-data from previous transformation
        step_inputs.append(
            WorkflowStepInput(
                id="input-data",
                source=f"{prev_transform_name}/output-data",
            )
        )

        # Link pool-xml-catalog from previous transformation
        step_inputs.append(
            WorkflowStepInput(
                id=POOL_XML,
                source=f"{prev_transform_name}/{POOL_XML_OUT}",
            )
        )
    else:
        # First transformation gets inputs from workflow level
        for input_id in workflow_inputs.keys():
            if input_id in ["production-id", "prod-job-id"]:
                # These are always from workflow level
                step_inputs.append(
                    WorkflowStepInput(
                        id=input_id,
                        source=input_id,
                    )
                )
            elif input_id in [EVENT_TYPE, RUN_NUMBER, FIRST_EVENT_NUMBER, NUMBER_OF_EVENTS, "histogram"]:
                # Gauss-specific inputs from workflow level
                step_inputs.append(
                    WorkflowStepInput(
                        id=input_id,
                        source=input_id,
                    )
                )
            elif input_id == POOL_XML:
                # Pool XML from workflow level (optional for first transformation)
                step_inputs.append(
                    WorkflowStepInput(
                        id=input_id,
                        source=input_id,
                    )
                )

    # Number of events is always passed through
    if NUMBER_OF_EVENTS not in [si.id for si in step_inputs]:
        step_inputs.append(
            WorkflowStepInput(
                id=NUMBER_OF_EVENTS,
                source=NUMBER_OF_EVENTS,
            )
        )

    # Build step outputs
    step_outputs = [
        WorkflowStepOutput(id="output-data"),
        WorkflowStepOutput(id="others"),
        WorkflowStepOutput(id=POOL_XML_OUT),
    ]

    return WorkflowStep(
        id=transform_name,
        run=transform_workflow,
        in_=step_inputs,
        out=step_outputs,
    )


def _getTransformationOutputs(
    steps: list[dict[str, Any]], step_names: list[str]
) -> list[WorkflowOutputParameter]:
    """Define outputs for a transformation sub-workflow.

    :param steps: List of steps in this transformation
    :param step_names: Names of the steps
    :return: List of workflow output parameters
    """
    workflow_outputs = []

    # Collect all output sources from all steps
    all_output_sources = []
    other_output_sources = []

    for step_index, step in enumerate(steps):
        step_name = step_names[step_index]
        all_output_sources.append(f"{step_name}/output-data")
        other_output_sources.append(f"{step_name}/others")

    # Output data files from all steps in this transformation
    workflow_outputs.append(
        WorkflowOutputParameter(
            type_="File[]",
            id="output-data",
            label="Output Data",
            outputSource=all_output_sources,
            linkMerge="merge_flattened",
        )
    )

    # Other outputs (logs, summaries) from all steps
    workflow_outputs.append(
        WorkflowOutputParameter(
            type_="File[]",
            id="others",
            label="Logs and summaries",
            outputSource=other_output_sources,
            linkMerge="merge_flattened",
        )
    )

    # Pool XML catalog from the last step
    if steps:
        last_step_name = step_names[-1]
        workflow_outputs.append(
            WorkflowOutputParameter(
                type_="File",
                id=POOL_XML_OUT,
                label="Pool XML Catalog",
                outputSource=f"{last_step_name}/{POOL_XML_OUT}",
            )
        )

    return workflow_outputs


def _getMainWorkflowOutputs(
    steps: list[dict[str, Any]],
    transforms: list[dict[str, Any]],
    transformation_names: list[str],
) -> list[WorkflowOutputParameter]:
    """Define outputs for the main production workflow.

    :param steps: All steps in the production
    :param transforms: All transformations in the production
    :param transformation_names: Names of all transformations
    :return: List of workflow output parameters
    """
    workflow_outputs = []

    # Collect outputs from transformations with visible output files
    visible_output_sources = []
    all_other_output_sources = []

    for transform_index, transform in enumerate(transforms):
        transform_name = transformation_names[transform_index]
        transform_step_indices = transform.get("steps", [])

        # Check if any step in this transformation has visible outputs
        has_visible_outputs = False
        for step_idx_1based in transform_step_indices:
            step = steps[step_idx_1based - 1]  # Convert to 0-indexed
            visible_outputs = [out for out in step.get("output", []) if out.get("visible", False)]
            if visible_outputs:
                has_visible_outputs = True
                break

        if has_visible_outputs:
            visible_output_sources.append(f"{transform_name}/output-data")

        # Collect all "others" outputs for logs
        all_other_output_sources.append(f"{transform_name}/others")

    # Main output data
    if visible_output_sources:
        workflow_outputs.append(
            WorkflowOutputParameter(
                type_="File[]",
                id="output-data",
                label="Output Data",
                outputSource=visible_output_sources,
                linkMerge="merge_flattened",
            )
        )

    # Logs and summaries from all transformations
    workflow_outputs.append(
        WorkflowOutputParameter(
            type_="File[]",
            id="others",
            label="Logs and summaries",
            outputSource=all_other_output_sources,
            linkMerge="merge_flattened",
        )
    )

    # Pool XML catalog from the last transformation
    if transformation_names:
        last_transform_name = transformation_names[-1]
        workflow_outputs.append(
            WorkflowOutputParameter(
                type_="File",
                id=POOL_XML_OUT,
                label="Pool XML Catalog",
                outputSource=f"{last_transform_name}/{POOL_XML_OUT}",
            )
        )

    return workflow_outputs

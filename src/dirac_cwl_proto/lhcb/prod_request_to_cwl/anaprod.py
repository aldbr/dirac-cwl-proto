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
"""Converter for LHCb Analysis Production requests to CWL workflows.

This converter takes analysis production request YAML files and converts them
into CWL format, handling DaVinci analysis jobs and ROOT file merging.

The converter:
- Parses AnalysisProduction YAML files
- Generates configuration for DaVinci and merge steps
- Creates CWL workflow with CommandLineTool for each step
- Handles input dataset queries from the Bookkeeping
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
    InlineJavascriptRequirement,
    MultipleInputFeatureRequirement,
    ResourceRequirement,
    StepInputExpressionRequirement,
    SubworkflowFeatureRequirement,
    Workflow,
    WorkflowInputParameter,
    WorkflowOutputParameter,
    WorkflowStep,
    WorkflowStepInput,
    WorkflowStepOutput,
)
from ruamel.yaml.scalarstring import LiteralScalarString

from .common import (
    sanitize_step_name,
    make_case_insensitive_glob,
    build_transformation_hints,
)


def fromProductionRequestYAMLToCWL(
    yaml_path: Path, production_name: str | None = None
) -> tuple[Workflow, dict[str, Any], dict[str, Any]]:
    """
    Convert an LHCb Analysis Production Request YAML file into a CWL Workflow.

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

    # Validate it's an analysis production
    if production_dict.get("type") != "AnalysisProduction":
        raise ValueError(
            f"Expected AnalysisProduction, got {production_dict.get('type')}"
        )

    # Build the CWL workflow
    return _buildCWLWorkflow(production_dict)


def _buildCWLWorkflow(
    production: dict[str, Any]
) -> tuple[Workflow, dict[str, Any], dict[str, Any]]:
    """Build a CWL workflow from an analysis production dictionary.

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
            "steps": list(range(len(steps))),  # 0-indexed
            "type": "WGProduction",
        }]

    # Build transformation sub-workflows
    transformation_workflows = []
    transformation_names = []

    for transform_index, transform in enumerate(transforms):
        transform_name = f"transformation_{transform_index + 1}"
        transformation_names.append(transform_name)

        # Build transformation sub-workflow
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
        ramMin=2048,
    )

    # Build detailed documentation
    input_dataset = production.get("input_dataset", {})
    conditions_dict = input_dataset.get("conditions_dict", {})

    doc_lines = [
        f"LHCb Analysis Production Workflow: {production_name}",
        "",
        "Metadata:",
    ]

    # Add input dataset information
    if input_dataset:
        doc_lines.append("  Input Dataset:")
        if "event_type" in input_dataset:
            doc_lines.append(f"    Event Type: {input_dataset['event_type']}")
        if conditions_dict:
            if "inProPass" in conditions_dict:
                doc_lines.append(f"    Processing Pass: {conditions_dict['inProPass']}")
            if "inFileType" in conditions_dict:
                doc_lines.append(f"    File Type: {conditions_dict['inFileType']}")
            if "configName" in conditions_dict and "configVersion" in conditions_dict:
                doc_lines.append(f"    Config: {conditions_dict['configName']}/{conditions_dict['configVersion']}")
        if "conditions_description" in input_dataset:
            doc_lines.append(f"    Conditions: {input_dataset['conditions_description']}")

    # Add workflow structure info
    doc_lines.append(f"")
    doc_lines.append(f"This workflow contains {len(transforms)} transformation(s).")
    doc_lines.append("")
    doc_lines.append("Generated by ProductionRequestToCWL converter")

    workflow_doc = "\n".join(doc_lines)

    # Create main workflow with extension fields for namespaces and input dataset
    extension_fields = {
        "$namespaces": {
            "dirac": "../../../schemas/dirac-metadata.json#/$defs/"
        },
        "$schemas": [
            "../../../schemas/dirac-metadata.json"
        ]
    }

    # Add input dataset as a machine-readable hint for BK query
    if input_dataset:
        extension_fields["dirac:inputDataset"] = {
            "eventType": input_dataset.get("event_type"),
            "conditionsDict": conditions_dict,
            "launchParameters": input_dataset.get("launch_parameters", {}),
            "conditionsDescription": input_dataset.get("conditions_description"),
        }

    workflow = Workflow(
        id=sanitize_step_name(production_name) or "analysis_production",
        label=production_name,
        doc=LiteralScalarString(workflow_doc),
        cwlVersion="v1.2",
        inputs=list(workflow_inputs.values()),
        outputs=workflow_outputs,
        steps=main_workflow_steps,
        requirements=[
            InlineJavascriptRequirement(),
            SubworkflowFeatureRequirement(),
            StepInputExpressionRequirement(),
            MultipleInputFeatureRequirement(),
            resource_requirement,
        ],
        extension_fields=extension_fields,
    )

    # Generate static inputs
    static_inputs = _getWorkflowStaticInputs(production)

    # Production metadata
    metadata = {
        "production_name": production_name,
        "production_type": "AnalysisProduction",
        "wg": production.get("wg"),
        "input_dataset": production.get("input_dataset"),
    }

    return workflow, static_inputs, metadata


def _getWorkflowInputs(production: dict[str, Any]) -> dict[str, WorkflowInputParameter]:
    """Define workflow-level input parameters for analysis production."""

    inputs = {}

    # Production identification inputs
    inputs["production-id"] = WorkflowInputParameter(
        id="production-id",
        type_="int",
        doc="Production ID",
        default=12345,
    )

    inputs["prod-job-id"] = WorkflowInputParameter(
        id="prod-job-id",
        type_="int",
        doc="Production Job ID",
        default=6789,
    )

    # Input data - for analysis productions, this comes from BK query
    inputs["input-data"] = WorkflowInputParameter(
        id="input-data",
        type_="File[]",
        doc="Input data files from Bookkeeping query",
    )

    return inputs


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
    step_indices = transform.get("steps", [])  # Already 0-indexed in YAML

    # Get the actual steps for this transformation
    transform_steps = [steps[idx] for idx in step_indices]

    # Build transformation-level inputs
    transformation_inputs = {}

    # Always include common inputs
    for input_id in ["production-id", "prod-job-id"]:
        if input_id in production_inputs:
            transformation_inputs[input_id] = production_inputs[input_id]

    # For non-first transformations, add input-data parameter to receive outputs from previous transformation
    # For first transformation, input-data comes from BK query
    if transform_index == 0:
        # First transformation receives input-data from workflow level
        transformation_inputs["input-data"] = production_inputs["input-data"]
    else:
        # Non-first transformations receive input-data from previous transformation
        transformation_inputs["input-data"] = WorkflowInputParameter(
            id="input-data",
            type_="File[]",
            doc="Input data files from previous transformation",
        )

    # Build CWL steps for this transformation
    cwl_steps = []
    step_names = []
    for local_step_index, global_step_index in enumerate(step_indices):
        step = steps[global_step_index]
        step_name = sanitize_step_name(step.get("name", f"step_{global_step_index}"))
        step_names.append(step_name)

        # Build the CommandLineTool for this step
        tool = _buildCommandLineTool(
            production, step, global_step_index, transform_index, len(step_indices)
        )

        # Build workflow step inputs
        step_inputs = _buildStepInputs(
            step, local_step_index, global_step_index, transformation_inputs, step_names
        )

        # Build workflow step outputs
        step_outputs = _buildStepOutputs(step)

        # Create workflow step
        cwl_step = WorkflowStep(
            id=step_name,
            run=tool,
            in_=step_inputs,
            out=step_outputs,
        )
        cwl_steps.append(cwl_step)

    # Build transformation outputs
    transformation_outputs = _getTransformationOutputs(
        steps, transform, step_names
    )

    # Build transformation hints
    hints = build_transformation_hints(transform)

    # Create transformation sub-workflow
    transformation_workflow = Workflow(
        id=f"transformation_{transform_index + 1}",
        label=f"Transformation {transform_index + 1}",
        doc=f"Transformation {transform_index + 1}: {transform.get('type', 'Processing')}\nContains {len(step_indices)} step(s)",
        cwlVersion="v1.2",
        inputs=list(transformation_inputs.values()),
        outputs=transformation_outputs,
        steps=cwl_steps,
        requirements=[
            InlineJavascriptRequirement(),
            StepInputExpressionRequirement(),
            MultipleInputFeatureRequirement(),
        ],
        hints=hints,
    )

    return transformation_workflow


def _buildCommandLineTool(
    production: dict[str, Any],
    step: dict[str, Any],
    step_index: int,
    transform_index: int,
    total_steps_in_transform: int,
) -> CommandLineTool:
    """Build a CommandLineTool for an analysis production step.

    :param production: The full production dictionary
    :param step: The step definition
    :param step_index: Global step index (0-based)
    :param transform_index: Index of the transformation this step belongs to
    :param total_steps_in_transform: Total number of steps in this transformation
    :return: A CommandLineTool for this step
    """
    # Generate prodConf for this step
    prodconf = _generateProdConf(production, step, step_index)

    step_name = sanitize_step_name(step.get("name", f"step_{step_index}"))
    application = step.get("application", {})

    if isinstance(application, dict):
        app_name = application.get("name", "unknown")
        app_version = application.get("version", "")
    else:
        # Sometimes application is just a string like "DaVinci/v44r11p6"
        parts = application.split("/")
        app_name = parts[0] if parts else "unknown"
        app_version = parts[1] if len(parts) > 1 else ""

    # Determine if this is a merge step
    is_merge = step.get("options", {}).get("entrypoint") == "LbExec:skim_and_merge"

    # Build input parameters
    input_parameters = _buildInputParameters(step, step_index, is_merge)

    # Build output parameters
    output_parameters = _buildOutputParameters(step)

    # Build InitialWorkDirRequirement with prodConf
    prodconf_filename = f"prodConf_{step_index + 1}.json"
    init_workdir_requirement = InitialWorkDirRequirement(
        listing=[
            Dirent(
                entryname=prodconf_filename,
                entry=LiteralScalarString(json.dumps(prodconf, indent=2)),
            )
        ]
    )

    # Build command
    baseCommand = ["lb-prod-run"]

    # Build arguments
    arguments = [
        f"--prodConf={prodconf_filename}",
    ]

    # Create the CommandLineTool
    requirements = [
        InlineJavascriptRequirement(),
        init_workdir_requirement,
    ]

    tool = CommandLineTool(
        id=f"{step_name}_tool",
        baseCommand=baseCommand,
        arguments=arguments,
        inputs=input_parameters,
        outputs=output_parameters,
        requirements=requirements,
    )

    return tool


def _generateProdConf(production: dict[str, Any], step: dict[str, Any], step_index: int) -> dict[str, Any]:
    """Generate prodConf JSON for an analysis production step."""

    application = step.get("application", {})
    if isinstance(application, dict):
        app_name = application.get("name", "unknown")
        app_version = application.get("version", "")
    else:
        parts = application.split("/")
        app_name = parts[0] if parts else "unknown"
        app_version = parts[1] if len(parts) > 1 else ""

    options = step.get("options", {})

    prodconf: dict[str, Any] = {
        "application": {
            "name": app_name,
            "version": app_version,
        },
        "step_id": step_index + 1,
        "step_name": step.get("name", f"step_{step_index}"),
    }

    # Add data packages
    data_pkgs = step.get("data_pkgs", [])
    if data_pkgs:
        prodconf["data_packages"] = data_pkgs

    # Handle different option formats
    if "entrypoint" in options:
        # This is a merge step
        prodconf["options"] = {
            "entrypoint": options["entrypoint"],
            "extra_args": options.get("extra_args", []),
            "extra_options": options.get("extra_options", {}),
        }
    elif "files" in options:
        # This is a WGProd step
        prodconf["options"] = {
            "files": options["files"],
            "format": options.get("format", "WGProd"),
        }
    else:
        prodconf["options"] = options

    # Add processing pass
    if "processing_pass" in step:
        prodconf["processing_pass"] = step["processing_pass"]

    return prodconf


def _buildInputParameters(
    step: dict[str, Any], step_index: int, is_merge: bool
) -> list[CommandInputParameter]:
    """Build input parameters for an analysis production step."""

    input_parameters = []

    # Production ID and job ID
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

    # Output prefix (computed from production-id and prod-job-id)
    input_parameters.append(
        CommandInputParameter(
            id="output-prefix",
            type_="string",
            inputBinding=CommandLineBinding(prefix="--output-prefix"),
        )
    )

    # Input files (if this is not the first step, or if it has dependencies)
    step_inputs = step.get("input", [])
    if step_inputs:
        input_parameters.append(
            CommandInputParameter(
                id="input-files",
                type_="string",
                inputBinding=CommandLineBinding(prefix="--input-files"),
            )
        )

    return input_parameters


def _buildOutputParameters(step: dict[str, Any]) -> list[CommandOutputParameter]:
    """Build output parameters for an analysis production step."""

    outputs = step.get("output", [])
    output_parameters = []

    # Output data files
    if outputs:
        # Get output file type from first output
        output_type = outputs[0].get("type", "ROOT")
        # Create glob pattern for output files
        glob_pattern = "*." + output_type.split(".")[-1].lower()

        output_parameters.append(
            CommandOutputParameter(
                id="output-data",
                type_="File[]",
                outputBinding=CommandOutputBinding(
                    glob=glob_pattern,
                ),
            )
        )

    # Others output for any additional files
    output_parameters.append(
        CommandOutputParameter(
            id="others",
            type_="File[]?",
            outputBinding=CommandOutputBinding(
                glob="*",
                outputEval=LiteralScalarString(
                    "$(self.filter(function(f) { "
                    "return !f.basename.endsWith('.root') && "
                    "!f.basename.startsWith('prodConf') && "
                    "!f.basename.startsWith('inputFiles') && "
                    "!f.basename.endsWith('.json'); }))"
                ),
            ),
        )
    )

    return output_parameters


def _buildStepInputs(
    step: dict[str, Any],
    step_index: int,
    global_step_index: int,
    workflow_inputs: dict[str, WorkflowInputParameter],
    step_names: list[str],
) -> list[WorkflowStepInput]:
    """Build workflow step inputs for an analysis production step."""

    step_inputs = []

    # Pass through production-id and prod-job-id
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

    # Compute output-prefix from production-id and prod-job-id
    step_inputs.append(
        WorkflowStepInput(
            id="output-prefix",
            source=["production-id", "prod-job-id"],
            valueFrom=f"$(self[0].toString().padStart(8, \"0\"))_$(self[1].toString().padStart(8, \"0\"))_{global_step_index + 1}",
        )
    )

    # Handle input files
    step_input_refs = step.get("input", [])
    if step_input_refs:
        # This step has inputs from a previous step
        # Find the source step
        source_step_idx = step_input_refs[0].get("step_idx")
        if source_step_idx is not None and source_step_idx < len(step_names):
            source_step_name = step_names[source_step_idx]

            # Create inputFiles manifest
            input_files_manifest = f"inputFiles_{global_step_index + 1}.txt"

            step_inputs.append(
                WorkflowStepInput(
                    id="input-files",
                    source=f"{source_step_name}/output-data",
                    valueFrom=LiteralScalarString(
                        f"${{{{ var files = self.map(f => f.path).join('\\n'); "
                        f"return {{{{ class: 'File', basename: '{input_files_manifest}', "
                        f"contents: files }}}}; }}}}"
                    ),
                )
            )

    return step_inputs


def _buildStepOutputs(step: dict[str, Any]) -> list[WorkflowStepOutput]:
    """Build workflow step outputs."""
    return [
        WorkflowStepOutput(id="output-data"),
        WorkflowStepOutput(id="others"),
    ]


def _getWorkflowStaticInputs(production: dict[str, Any]) -> dict[str, Any]:
    """Generate static input values for the workflow."""

    static_inputs = {
        "production-id": 12345,
        "prod-job-id": 6789,
    }

    # Note: input-data would typically come from a Bookkeeping query
    # For now, we'll leave it as a required input
    static_inputs["input-data"] = []

    return static_inputs


def _buildMainWorkflowStep(
    transform_name: str,
    transform_workflow: Workflow,
    transform: dict[str, Any],
    transform_index: int,
    workflow_inputs: dict[str, WorkflowInputParameter],
    transformation_names: list[str],
) -> WorkflowStep:
    """Build a step in the main workflow that references a transformation sub-workflow."""

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
    else:
        # First transformation gets input-data from workflow inputs
        step_inputs.append(
            WorkflowStepInput(
                id="input-data",
                source="input-data",
            )
        )

    # Always pass through common workflow inputs
    for input_id in ["production-id", "prod-job-id"]:
        if input_id in workflow_inputs:
            step_inputs.append(
                WorkflowStepInput(
                    id=input_id,
                    source=input_id,
                )
            )

    # Build step outputs
    step_outputs = [
        WorkflowStepOutput(id="output-data"),
        WorkflowStepOutput(id="others"),
    ]

    # Create the workflow step
    return WorkflowStep(
        id=transform_name,
        run=transform_workflow,
        in_=step_inputs,
        out=step_outputs,
    )


def _getTransformationOutputs(
    steps: list[dict[str, Any]], transform: dict[str, Any], step_names: list[str]
) -> list[WorkflowOutputParameter]:
    """Build outputs for a transformation sub-workflow."""

    step_indices = transform.get("steps", [])

    # Collect all output sources from all steps
    output_sources = []
    for step_index in step_indices:
        step_name = step_names[step_index] if step_index < len(step_names) else f"step_{step_index}"
        output_sources.append(f"{step_name}/output-data")

    # Output data files from all steps in this transformation
    outputs = [
        WorkflowOutputParameter(
            id="output-data",
            type_="File[]",
            outputSource=output_sources,
            linkMerge="merge_flattened",
        ),
        WorkflowOutputParameter(
            id="others",
            type_={"type": "array", "items": ["File", "null"]},
            outputSource=[f"{step_names[idx]}/others" for idx in step_indices if idx < len(step_names)],
            linkMerge="merge_flattened",
        ),
    ]

    return outputs


def _getMainWorkflowOutputs(
    steps: list[dict[str, Any]],
    transforms: list[dict[str, Any]],
    transformation_names: list[str],
) -> list[WorkflowOutputParameter]:
    """Build outputs for the main workflow."""

    # Output comes from the last transformation
    last_transform = transformation_names[-1] if transformation_names else "transformation_1"

    return [
        WorkflowOutputParameter(
            id="output-data",
            label="Output Data",
            type_="File[]",
            outputSource=f"{last_transform}/output-data",
        ),
        WorkflowOutputParameter(
            id="others",
            label="Logs and summaries",
            type_={"type": "array", "items": ["File", "null"]},
            outputSource=f"{last_transform}/others",
        ),
    ]

"""
Utils.
"""
import importlib
from pathlib import Path

from cwl_utils.parser import load_document_by_uri
from cwl_utils.parser.cwl_v1_2 import (
    Workflow,
)

from dirac_cwl_proto.metadata_models import IMetadataModel
from dirac_cwl_proto.submission_models import (
    JobSubmissionModel,
    TransformationSubmissionModel,
)


def dash_to_snake_case(name):
    """Converts a string from dash-case to snake_case."""
    return name.replace("-", "_")


def snake_case_to_dash(name):
    """Converts a string from snake_case to dash-case."""
    return name.replace("_", "-")


def load_workflow(file_uri):
    """
    Iteratively load a CWL workflow and all its subworkflows using a stack.

    This implementation ensures that all subworkflows are fully resolved
    from the most nested (leaf) to the main workflow.

    Args:
        file_uri (str): Path to the main CWL workflow file.

    Returns:
        Workflow: Fully resolved workflow object.

    Raises:
        FileNotFoundError: If any subworkflow file is not found.
    """
    # Stack for processing workflows (start with the root file)
    stack = [(file_uri, None)]  # (workflow file path, parent step reference)
    loaded_workflows = {}  # Cache loaded workflows by their file paths

    while stack:
        current_uri, parent_step = stack.pop()

        # Check if workflow is already loaded
        if current_uri in loaded_workflows:
            # Assign the cached workflow to the parent step if applicable
            if parent_step is not None:
                parent_step.run = loaded_workflows[current_uri]
            continue

        # Load the current workflow
        workflow = load_document_by_uri(current_uri)

        # Cache the loaded workflow
        loaded_workflows[current_uri] = workflow

        # If it's not a Workflow object, assign it directly to the parent and continue
        if not isinstance(workflow, Workflow):
            if parent_step is not None:
                parent_step.run = workflow
            continue

        # Defer assigning the workflow to its parent until its steps are resolved
        if parent_step is not None:
            parent_step.run = workflow

        # Process steps and push subworkflows onto the stack
        for wf_step in workflow.steps:
            if wf_step.run and isinstance(wf_step.run, str):  # If `run` is a file path
                subworkflow_path = Path(wf_step.run.replace("file://", "")).resolve()

                if not subworkflow_path.exists():
                    raise FileNotFoundError(
                        f"Subworkflow file not found: {subworkflow_path}"
                    )

                # Add the subworkflow to the stack for resolution
                stack.append((str(subworkflow_path), wf_step))

    return loaded_workflows[file_uri]


def _get_metadata(
    submitted: JobSubmissionModel | TransformationSubmissionModel,
) -> IMetadataModel:
    """Get the metadata class for the transformation.

    :param transformation: The transformation to get the metadata for

    :return: The metadata class
    """
    if not submitted.metadata:
        raise RuntimeError("Transformation metadata is not set.")

    # Get the inputs
    inputs = {}
    for input in submitted.task.inputs:
        input_name = input.id.split("#")[-1].split("/")[-1]

        input_value = input.default
        if (
            hasattr(submitted, "parameters")
            and submitted.parameters
            and submitted.parameters[0]
        ):
            input_value = submitted.parameters[0].cwl.get(input_name, input_value)

        inputs[input_name] = input_value

    # Merge the inputs with the query params
    if submitted.metadata.query_params:
        inputs.update(submitted.metadata.query_params)

    # Get the metadata class
    try:
        module = importlib.import_module("dirac_cwl_proto.metadata_models")
        metadata_class = getattr(module, submitted.metadata.type)
    except AttributeError:
        raise RuntimeError(
            f"Metadata class {submitted.metadata.type} not found."
        ) from None

    # Convert the inputs to snake case
    params = {dash_to_snake_case(key): value for key, value in inputs.items()}
    return metadata_class(**params)

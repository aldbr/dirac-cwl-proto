"""
CLI interface to run a CWL workflow from end to end (production/transformation/job).
"""
import importlib

from dirac_cwl_proto.metadata_models import IMetadataModel
from dirac_cwl_proto.submission_models import (
    JobSubmissionModel,
    TransformationSubmissionModel,
)

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------


def dash_to_snake_case(name):
    """Converts a string from dash-case to snake_case."""
    return name.replace("-", "_")


def snake_case_to_dash(name):
    """Converts a string from snake_case to dash-case."""
    return name.replace("_", "-")


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
    return metadata_class(**inputs)

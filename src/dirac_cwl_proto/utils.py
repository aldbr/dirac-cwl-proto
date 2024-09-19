"""
CLI interface to run a CWL workflow from end to end (production/transformation/job).
"""
import importlib

from dirac_cwl_proto.metadata_models import IMetadataModel
from dirac_cwl_proto.submission_models import (
    JobSubmissionModel,
    TransformationSubmissionModel,
)

# class CWLBaseModel(BaseModel):
#     """Base class for CWL models."""

#     workflow_path: str
#     workflow: CommandLineTool | Workflow | None = None

#     metadata_path: str
#     metadata_type: str
#     metadata: "IMetadataModel" | None = None

#     id: int | None = None
#     _id_counter = 0  # Class variable to keep track of the last assigned ID
#     _id_lock = PrivateAttr(
#         default_factory=Lock
#     )  # Lock to ensure thread-safe access to _id_counter

#     def __init__(self, **data):
#         super().__init__(**data)
#         with self._id_lock:  # Acquire lock for thread-safe ID increment
#             self.id = self._id_counter
#             self._id_counter += 1

#     @validator("workflow_path")
#     def validate_workflow(cls, workflow_path):
#         if not validate_cwl(workflow_path):
#             raise ValueError(f"Invalid CWL file: {workflow_path}")
#         return workflow_path

#     @validator("metadata_path")
#     def validate_metadata(cls, metadata_path):
#         if metadata_path and not os.path.isfile(metadata_path):
#             raise ValueError(f"Metadata file does not exist: {metadata_path}")
#         return metadata_path

#     @model_validator(mode="after")
#     def load_workflow_metadata(self):
#         """Load the workflow and metadata files."""
#         from .metadata_models import (
#             BasicMetadataModel,
#             LHCbMetadataModel,
#             MacobacMetadataModel,
#             MandelBrotMetadataModel,
#         )
#         metadata_models = {
#             "basic": BasicMetadataModel,
#             "macobac": MacobacMetadataModel,
#             "lhcb": LHCbMetadataModel,
#             "mandelbrot": MandelBrotMetadataModel,
#         }
#         try:
#             self.workflow = load_document_by_uri(self.workflow_path)

#             with open(self.metadata_path, "r") as file:
#                 metadata = YAML(typ="safe").load(file)

#             # Adapt the metadata to the expected format if needed
#             for key, value in metadata.items():
#                 if isinstance(value, dict) and (
#                     value["class"] == "File" or value["class"] == "Directory"
#                 ):
#                     metadata[key] = value["path"]

#             # Dynamically create a metadata model based on the metadata type
#             self.metadata = metadata_models[self.metadata_type](  # noqa
#                 **{dash_to_snake_case(k): v for k, v in metadata.items()}
#             )
#         except Exception as e:
#             raise ValidationError(f"Failed to load workflow and metadata: {e}") from e

#     class Config:
#         # Allow arbitrary types to be passed to the model
#         arbitrary_types_allowed = True


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
        input_name = input.id.split("#")[1]

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

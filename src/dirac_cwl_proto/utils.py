"""
Utils.
"""

from typing import Any

from dirac_cwl_proto.submission_models import (
    JobDescriptionModel,
    JobMetadataModel,
)


def extract_dirac_hints(cwl: Any) -> tuple[JobMetadataModel, JobDescriptionModel]:
    """Extract `dirac:metadata` and `dirac:description` from a CWL document.

    Parameters
    - cwl: either a parsed CWL dict or a Path to a CWL file.

    Returns a tuple (JobMetadataModel, JobDescriptionModel) — both models are
    validated Pydantic descriptors. Unknown hints are logged as warnings.
    """
    metadata = JobMetadataModel()
    description = JobDescriptionModel()

    hints = cwl.hints or []
    for hint in hints:
        hint_class = hint.get("class")
        hint_body = {k: v for k, v in hint.items() if k != "class"}

        if hint_class == "dirac:metadata":
            metadata = metadata.model_copy(update=hint_body)

        if hint_class == "dirac:description":
            description = description.model_copy(update=hint_body)

    return metadata, description

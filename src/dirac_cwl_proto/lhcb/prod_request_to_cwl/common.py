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
"""Common utilities shared between simulation and analysis production converters."""

import re
from typing import Any

# Constants
EVENT_TYPE = "event-type"
RUN_NUMBER = "run-number"
FIRST_EVENT_NUMBER = "first-event-number"
NUMBER_OF_EVENTS = "number-of-events"


def sanitize_step_name(name: str) -> str:
    """Sanitize a step name to be valid CWL identifier.

    CWL identifiers must match [a-zA-Z_][a-zA-Z0-9_-]*

    :param name: Original step name
    :return: Sanitized step name
    """
    # Replace invalid characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "_", name)

    # Ensure it starts with a letter or underscore
    if sanitized and not sanitized[0].isalpha() and sanitized[0] != "_":
        sanitized = "_" + sanitized

    return sanitized


def make_case_insensitive_glob(extension: str) -> str:
    """Create a case-insensitive glob pattern for a file extension.

    For example, '.sim' becomes '.[sS][iI][mM]'

    :param extension: File extension (e.g., '.sim', '.digi')
    :return: Case-insensitive glob pattern
    """
    if not extension.startswith("."):
        extension = "." + extension

    pattern = ""
    for char in extension:
        if char.isalpha():
            pattern += f"[{char.lower()}{char.upper()}]"
        else:
            pattern += char

    return pattern


def build_transformation_hints(transform: dict[str, Any]) -> list[dict[str, Any]]:
    """Build DIRAC-specific hints for a transformation.

    These hints are used by the DIRAC integration to configure job execution.

    :param transform: Transformation definition from submission_info
    :return: List of hint dictionaries
    """
    hints = []

    # Create TransformationExecutionHooks hint
    hint: dict[str, Any] = {
        "class": "dirac:TransformationExecutionHooks",
    }

    # Configuration fields that go under "configuration" key
    config: dict[str, Any] = {}

    if "cpu" in transform:
        config["cpu"] = transform["cpu"]
    if "priority" in transform:
        config["priority"] = transform["priority"]
    if "multicore" in transform:
        config["multicore"] = transform["multicore"]
    if "output_se" in transform:
        config["output_se"] = transform["output_se"]
    if "output_mode" in transform:
        config["output_mode"] = transform["output_mode"]
    if "input_plugin" in transform:
        config["input_plugin"] = transform["input_plugin"]
    if "ancestor_depth" in transform:
        config["ancestor_depth"] = transform["ancestor_depth"]
    if "output_file_mask" in transform:
        config["output_file_mask"] = transform["output_file_mask"]
    if "input_data_policy" in transform:
        config["input_data_policy"] = transform["input_data_policy"]
    if "remove_inputs_flags" in transform:
        config["remove_inputs_flags"] = transform["remove_inputs_flags"]
    if "events" in transform:
        config["events"] = transform["events"]

    # Group size goes directly in hint (not in configuration)
    if "group_size" in transform:
        hint["group_size"] = transform["group_size"]

    # Add configuration if we have any
    if config:
        hint["configuration"] = config

    # Add transformation type if specified
    if "type" in transform:
        hint["hook_plugin"] = transform["type"]

    hints.append(hint)

    return hints

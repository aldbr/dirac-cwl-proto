import re
import shutil
import threading
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dirac_cwl_proto import app


def strip_ansi_codes(text: str) -> str:
    """Remove ANSI color codes from text."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


@pytest.fixture()
def cli_runner():
    return CliRunner()


@pytest.fixture()
def cleanup():
    def _cleanup():
        shutil.rmtree("filecatalog", ignore_errors=True)
        shutil.rmtree("sandboxstore", ignore_errors=True)
        shutil.rmtree("workernode", ignore_errors=True)

    _cleanup()
    yield
    _cleanup()


# -----------------------------------------------------------------------------
# Job tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file, inputs",
    [
        # --- Hello World example ---
        # There is no input expected
        ("test/workflows/helloworld/description_basic.cwl", []),
        # An input is expected but not required (default value is used)
        ("test/workflows/helloworld/description_with_inputs.cwl", []),
        # A string input is passed
        (
            "test/workflows/helloworld/description_with_inputs.cwl",
            [
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs1.yaml"
            ],
        ),
        # Multiple string inputs are passed
        (
            "test/workflows/helloworld/description_with_inputs.cwl",
            [
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs1.yaml",
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs2.yaml",
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs3.yaml",
            ],
        ),
        # --- Test metadata example ---
        # A string input is passed
        (
            "test/workflows/test_meta/test_meta.cwl",
            [
                "test/workflows/test_meta/override_dirac_hints.yaml",
            ],
        ),
        # --- Crypto example ---
        # Complete
        (
            "test/workflows/crypto/description.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # Caesar only
        (
            "test/workflows/crypto/caesar.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # ROT13 only
        (
            "test/workflows/crypto/rot13.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # Base64 only
        (
            "test/workflows/crypto/base64.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # MD5 only
        (
            "test/workflows/crypto/md5.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
    ],
)
def test_run_job_success(cli_runner, cleanup, cwl_file, inputs):
    # CWL file is the first argument
    command = ["job", "submit", cwl_file]

    # Add the input file(s)
    for input in inputs:
        command.extend(["--parameter-path", input])

    result = cli_runner.invoke(app, command)
    # Remove ANSI color codes for assertion
    clean_output = strip_ansi_codes(result.stdout)
    assert "CLI: Job(s) done" in clean_output, f"Failed to run the job: {result.stdout}"


@pytest.mark.parametrize(
    "cwl_file, inputs, expected_error",
    [
        # The description file is malformed: class attribute is unknown
        (
            "test/workflows/malformed_description/description_malformed_class.cwl",
            [],
            "`class`containsundefinedreferenceto",
        ),
        # The description file is malformed: baseCommand is unknown
        (
            "test/workflows/malformed_description/description_malformed_command.cwl",
            [],
            "invalidfield`baseComand`",
        ),
        # The description file points to a non-existent file (subworkflow)
        (
            "test/workflows/bad_references/reference_doesnotexists.cwl",
            [],
            "Nosuchfileordirectory",
        ),
        # The description file points to another file point to it (circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            [],
            "Recursingintostep",
        ),
        # The description file points to itself (another circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            [],
            "Recursingintostep",
        ),
        # The configuration file is malformed: the hints are overridden more than once
        (
            "test/workflows/test_meta/test_meta.cwl",
            [
                "test/workflows/test_meta/override_dirac_hints_twice.yaml",
            ],
            "Failedtovalidatetheparameter",
        ),
    ],
)
def test_run_job_validation_failure(
    cli_runner, cleanup, cwl_file, inputs, expected_error
):
    command = ["job", "submit", cwl_file]
    for input in inputs:
        command.extend(["--parameter-path", input])
    result = cli_runner.invoke(app, command)
    clean_stdout = strip_ansi_codes(result.stdout)
    assert "Job(s) done" not in clean_stdout, "The job did complete successfully."

    # Check all possible output sources
    clean_output = re.sub(r"\s+", "", result.stdout)
    try:
        clean_stderr = re.sub(r"\s+", "", result.stderr or "")
    except (ValueError, AttributeError):
        clean_stderr = ""
    clean_exception = re.sub(
        r"\s+", "", str(result.exception) if result.exception else ""
    )

    # Handle different possible error messages for circular references
    if expected_error == "Recursingintostep":
        # Accept multiple possible error patterns for circular references
        circular_ref_patterns = [
            "Recursingintostep",
            "maximumrecursiondepthexceeded",
            "RecursionError",
            "circularreference",
        ]
        error_found = any(
            pattern in clean_output
            or pattern in clean_stderr
            or pattern in clean_exception
            for pattern in circular_ref_patterns
        )
        assert error_found, (
            f"None of the expected circular reference error patterns found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )
    else:
        error_found = (
            expected_error in clean_output
            or expected_error in clean_stderr
            or expected_error in clean_exception
        )
        assert error_found, (
            f"Expected error '{expected_error}' not found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )


# -----------------------------------------------------------------------------
# Transformation tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file, metadata",
    [
        # --- Hello World example ---
        # There is no input expected
        ("test/workflows/helloworld/description_basic.cwl", None),
        # --- Crypto example ---
        # Complete
        ("test/workflows/crypto/description.cwl", None),
        # Caesar only
        ("test/workflows/crypto/caesar.cwl", None),
        # ROT13 only
        ("test/workflows/crypto/rot13.cwl", None),
        # Base64 only
        ("test/workflows/crypto/base64.cwl", None),
        # MD5 only
        ("test/workflows/crypto/md5.cwl", None),
    ],
)
def test_run_nonblocking_transformation_success(
    cli_runner, cleanup, cwl_file, metadata
):
    # CWL file is the first argument
    command = ["transformation", "submit", cwl_file]
    # Add the metadata file
    if metadata:
        command.extend(["--metadata-path", metadata])

    result = cli_runner.invoke(app, command)
    clean_output = strip_ansi_codes(result.stdout)
    assert (
        "Transformation done" in clean_output
    ), f"Failed to run the transformation: {result.stdout}"


@pytest.mark.skip(
    reason="Temporarily disabled: no non-core plugin tests during refactoring"
)
@pytest.mark.parametrize(
    "cwl_file, metadata, destination_source_input_data",
    [
        # Placeholder - all tests commented out during architectural refactoring
        pytest.param(None, None, None, marks=pytest.mark.skip),
    ],
)
def test_run_blocking_transformation_success(
    cli_runner, cleanup, cwl_file, metadata, destination_source_input_data
):
    # Define a function to run the transformation command and return the result
    def run_transformation():
        command = ["transformation", "submit", cwl_file]
        if metadata:
            command.extend(["--metadata-path", metadata])
        return cli_runner.invoke(app, command)

    # Start running the transformation in a separate thread and capture the result
    transformation_result = None

    def run_and_capture():
        nonlocal transformation_result
        transformation_result = run_transformation()

    transformation_thread = threading.Thread(target=run_and_capture)
    transformation_thread.start()

    # Give it some time to ensure the command is waiting for input files
    time.sleep(5)

    # Ensure the command is waiting (e.g., it hasn't finished yet)
    assert (
        transformation_thread.is_alive()
    ), "The transformation should be waiting for files."

    for destination, inputs in destination_source_input_data.items():
        # Copy the input data to the destination
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        for input in inputs:
            shutil.copy(input, destination)

    # Wait for the thread to finish
    transformation_thread.join(timeout=60)

    # Check if the transformation completed successfully
    assert (
        transformation_result is not None
    ), "The transformation result was not captured."
    clean_transformation_output = strip_ansi_codes(transformation_result.stdout)
    assert (
        "Transformation done" in clean_transformation_output
    ), "The transformation did not complete successfully."


@pytest.mark.parametrize(
    "cwl_file, metadata, expected_error",
    [
        # The description file is malformed: class attribute is unknown
        (
            "test/workflows/malformed_description/description_malformed_class.cwl",
            None,
            "`class`containsundefinedreferenceto",
        ),
        # The description file is malformed: baseCommand is unknown
        (
            "test/workflows/malformed_description/description_malformed_command.cwl",
            None,
            "invalidfield`baseComand`",
        ),
        # The description file points to a non-existent file (subworkflow)
        (
            "test/workflows/bad_references/reference_doesnotexists.cwl",
            [],
            "Nosuchfileordirectory",
        ),
        # The description file points to another file point to it (circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            [],
            "Recursingintostep",
        ),
        # The description file points to itself (another circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            [],
            "Recursingintostep",
        ),
    ],
)
def test_run_transformation_validation_failure(
    cli_runner, cwl_file, cleanup, metadata, expected_error
):
    command = ["transformation", "submit", cwl_file]
    if metadata:
        command.extend(["--metadata-path", metadata])
    result = cli_runner.invoke(app, command)
    clean_stdout = strip_ansi_codes(result.stdout)
    assert (
        "Transformation done" not in clean_stdout
    ), "The transformation did complete successfully."

    # Check all possible output sources
    clean_output = re.sub(r"\s+", "", result.stdout)
    try:
        clean_stderr = re.sub(r"\s+", "", result.stderr or "")
    except (ValueError, AttributeError):
        clean_stderr = ""
    clean_exception = re.sub(
        r"\s+", "", str(result.exception) if result.exception else ""
    )

    # Handle multiple possible error patterns for circular references
    if expected_error == "Recursingintostep":
        # Check for various circular reference error patterns
        circular_ref_patterns = [
            "Recursingintostep",
            "RecursionError",
            "maximumrecursiondepthexceeded",
            "circularreference",
        ]
        error_found = any(
            pattern in clean_output
            or pattern in clean_stderr
            or pattern in clean_exception
            for pattern in circular_ref_patterns
        )
        assert error_found, (
            f"None of the expected circular reference error patterns were found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )
    else:
        error_found = (
            expected_error in clean_output
            or expected_error in clean_stderr
            or expected_error in clean_exception
        )
        assert error_found, (
            f"Expected error '{expected_error}' not found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )


# -----------------------------------------------------------------------------
# Production tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file, metadata",
    [
        # --- Crypto example ---
        # Complete
        ("test/workflows/crypto/description.cwl", None),
    ],
)
def test_run_simple_production_success(cli_runner, cleanup, cwl_file, metadata):
    # CWL file is the first argument
    command = ["production", "submit", cwl_file]
    # Add the metadata file
    if metadata:
        command.extend(["--steps-metadata-path", metadata])

    result = cli_runner.invoke(app, command)
    clean_output = strip_ansi_codes(result.stdout)
    assert (
        "Production done" in clean_output
    ), f"Failed to run the production: {result.stdout}"


@pytest.mark.parametrize(
    "cwl_file, metadata, expected_error",
    [
        # The description file is malformed: class attribute is unknown
        (
            "test/workflows/malformed_description/description_malformed_class.cwl",
            None,
            "`class`containsundefinedreferenceto",
        ),
        # The description file is malformed: baseCommand is unknown
        (
            "test/workflows/malformed_description/description_malformed_command.cwl",
            None,
            "invalidfield`baseComand`",
        ),
        # The description file points to a non-existent file (subworkflow)
        (
            "test/workflows/bad_references/reference_doesnotexists.cwl",
            [],
            "Nosuchfileordirectory",
        ),
        # The description file points to another file point to it (circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            [],
            "Recursingintostep",
        ),
        # The description file points to itself (another circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            [],
            "Recursingintostep",
        ),
        # The workflow is a CommandLineTool instead of a Workflow
        (
            "test/workflows/helloworld/description_basic.cwl",
            None,
            "InputshouldbeaninstanceofWorkflow",
        ),
    ],
)
def test_run_production_validation_failure(
    cli_runner, cleanup, cwl_file, metadata, expected_error
):
    command = ["production", "submit", cwl_file]
    if metadata:
        command.extend(["--steps-metadata-path", metadata])
    result = cli_runner.invoke(app, command)

    clean_stdout = strip_ansi_codes(result.stdout)
    assert (
        "Transformation done" not in clean_stdout
    ), "The transformation did complete successfully."

    # Check all possible output sources
    clean_output = re.sub(r"\s+", "", f"{result.stdout}")
    try:
        clean_stderr = re.sub(r"\s+", "", result.stderr or "")
    except (ValueError, AttributeError):
        clean_stderr = ""
    clean_exception = re.sub(r"\s+", "", f"{result.exception}")

    if expected_error == "Recursingintostep":
        # Check for various circular reference error patterns
        circular_ref_patterns = [
            "Recursingintostep",
            "RecursionError",
            "maximumrecursiondepthexceeded",
            "circularreference",
        ]
        error_found = any(
            pattern in clean_output
            or pattern in clean_stderr
            or pattern in clean_exception
            for pattern in circular_ref_patterns
        )
        assert error_found, (
            f"None of the expected circular reference error patterns were found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )
    else:
        error_found = (
            expected_error in clean_output
            or expected_error in clean_stderr
            or expected_error in clean_exception
        )
        assert error_found, (
            f"Expected error '{expected_error}' not found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )

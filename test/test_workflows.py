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
        # --- Pi example ---
        # Complete
        (
            "test/workflows/pi/description.cwl",
            ["test/workflows/pi/type_dependencies/job/inputs-pi_complete.yaml"],
        ),
        # Simulate only
        ("test/workflows/pi/pisimulate.cwl", []),
        # Gather only
        (
            "test/workflows/pi/pigather.cwl",
            ["test/workflows/pi/type_dependencies/job/inputs-pi_gather.yaml"],
        ),
        # --- Merge example ---
        # Complete
        ("test/workflows/merge/description.cwl", []),
        # --- LHCb example ---
        # Complete
        (
            "test/workflows/lhcb/description.cwl",
            ["test/workflows/lhcb/type_dependencies/job/inputs-lhcb_complete.yaml"],
        ),
        # Simulate only
        (
            "test/workflows/lhcb/lhcbsimulate.cwl",
            ["test/workflows/lhcb/type_dependencies/job/inputs-lhcb_simulate.yaml"],
        ),
        # Reconstruct only
        (
            "test/workflows/lhcb/lhcbreconstruct.cwl",
            ["test/workflows/lhcb/type_dependencies/job/inputs-lhcb_reconstruct.yaml"],
        ),
        # --- Mandelbrot example ---
        # Complete
        (
            "test/workflows/mandelbrot/description.cwl",
            [
                "test/workflows/mandelbrot/type_dependencies/job/inputs-mandelbrot_complete.yaml"
            ],
        ),
        # Image production only
        ("test/workflows/mandelbrot/image-prod.cwl", []),
        # Image merge only
        (
            "test/workflows/mandelbrot/image-merge.cwl",
            [
                "test/workflows/mandelbrot/type_dependencies/job/inputs-mandelbrot_imagemerge.yaml"
            ],
        ),
        # --- Gaussian fit example ---
        # Data generation only
        (
            "test/workflows/gaussian_fit/data_generation/data-generation.cwl",
            [
                "test/workflows/gaussian_fit/type_dependencies/job/inputs-data-generation.yaml"
            ],
        ),
        # Gaussian fit only
        (
            "test/workflows/gaussian_fit/gaussian_fit/gaussian-fit.cwl",
            [
                "test/workflows/gaussian_fit/type_dependencies/job/inputs-gaussian-fit.yaml"
            ],
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
        # --- Pi example ---
        # There is no input expected
        ("test/workflows/pi/pisimulate.cwl", None),
        # --- Pi v2 example ---
        # There is no input expected
        ("test/workflows/merge/pisimulate_v2.cwl", None),
        # --- LHCb example ---
        ("test/workflows/lhcb/lhcbsimulate.cwl", None),
        # --- Mandelbrot example ---
        ("test/workflows/mandelbrot/image-prod.cwl", None),
        # --- Gaussian fit example ---
        # Data generation workflow
        (
            "test/workflows/gaussian_fit/data_generation/data-generation-workflow.cwl",
            None,
        ),
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


@pytest.mark.parametrize(
    "cwl_file, metadata, destination_source_input_data",
    [
        # --- Pi example ---
        (
            "test/workflows/pi/pigather.cwl",
            None,
            {
                "filecatalog/pi/100": [
                    "test/workflows/pi/type_dependencies/job/result_1.sim",
                    "test/workflows/pi/type_dependencies/job/result_2.sim",
                    "test/workflows/pi/type_dependencies/job/result_3.sim",
                    "test/workflows/pi/type_dependencies/job/result_4.sim",
                    "test/workflows/pi/type_dependencies/job/result_5.sim",
                ]
            },
        ),
        # --- LHCb example ---
        (
            "test/workflows/lhcb/lhcbreconstruct.cwl",
            None,
            {
                "filecatalog/lhcb/456/123/simulation": [
                    "test/workflows/lhcb/type_dependencies/job/Gauss_123_456_1.sim",
                    "test/workflows/lhcb/type_dependencies/job/Gauss_456_456_1.sim",
                    "test/workflows/lhcb/type_dependencies/job/Gauss_789_456_1.sim",
                ]
            },
        ),
        # --- Mandelbrot example ---
        (
            "test/workflows/mandelbrot/image-merge.cwl",
            None,
            {
                "filecatalog/mandelbrot/images/raw/1920x1080/": [
                    "test/workflows/mandelbrot/type_dependencies/transformation/data_1.txt",
                    "test/workflows/mandelbrot/type_dependencies/transformation/data_2.txt",
                    "test/workflows/mandelbrot/type_dependencies/transformation/data_3.txt",
                ]
            },
        ),
        # Gaussian fit workflow
        (
            "test/workflows/gaussian_fit/gaussian_fit/gaussian-fit-workflow.cwl",
            None,
            {
                "filecatalog/gaussian_fit/data-generation-1/": [
                    "test/workflows/gaussian_fit/type_dependencies/transformation/data-generation-1/data_gen1.txt",
                ],
                "filecatalog/gaussian_fit/data-generation-2/": [
                    "test/workflows/gaussian_fit/type_dependencies/transformation/data-generation-2/data_gen2.txt",
                ],
            },
        ),
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
        # --- Pi example ---
        # There is no input expected
        (
            "test/workflows/pi/description.cwl",
            "test/workflows/pi/type_dependencies/production/metadata-pi_complete.yaml",
        ),
        # --- Merge example ---
        # There is no input expected
        (
            "test/workflows/merge/description.cwl",
            "test/workflows/merge/type_dependencies/production/metadata-merge_complete.yaml",
        ),
        # --- LHCb example ---
        # Complete
        (
            "test/workflows/lhcb/description.cwl",
            "test/workflows/lhcb/type_dependencies/production/metadata-lhcb_complete.yaml",
        ),
        # --- Mandelbrot example ---
        (
            "test/workflows/mandelbrot/description.cwl",
            "test/workflows/mandelbrot/type_dependencies/production/metadata-mandelbrot_complete.yaml",
        ),
        # --- Gaussian fit example ---
        # Complete
        (
            "test/workflows/gaussian_fit/main-workflow.cwl",
            "test/workflows/gaussian_fit/type_dependencies/production/metadata-gaussian-fit-complete.yaml",
        ),
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
        # The metadata has an unexistent step name
        (
            "test/workflows/mandelbrot/description.cwl",
            "test/workflows/mandelbrot/type_dependencies/production/malformed-wrong-stepname_metadata-mandelbrot_complete.yaml",
            "Thefollowingstepsaremissingfromthetaskworkflow:{'this-step-doesnot-exist'}",
        ),
        # The metadata has an unexistent type
        (
            "test/workflows/mandelbrot/description.cwl",
            "test/workflows/mandelbrot/type_dependencies/production/malformed-nonexisting-type_metadata-mandelbrot_complete.yaml",
            "Unknownexecutionhooksplugin:'MandelBrotDoesNotExist'",
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

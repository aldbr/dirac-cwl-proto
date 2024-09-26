import re
import shutil
import threading
import time
from pathlib import Path

import pytest
from ruamel.yaml import YAML
from typer.testing import CliRunner

from dirac_cwl_proto import app


@pytest.fixture()
def cli_runner():
    return CliRunner()


@pytest.fixture()
def cleanup():
    shutil.rmtree("filecatalog", ignore_errors=True)
    shutil.rmtree("sandboxstore", ignore_errors=True)
    # crypto results
    Path("base64_result.txt").unlink(missing_ok=True)
    Path("caesar_result.txt").unlink(missing_ok=True)
    Path("md5_result.txt").unlink(missing_ok=True)
    Path("rot13_result.txt").unlink(missing_ok=True)

    yield
    shutil.rmtree("filecatalog", ignore_errors=True)
    shutil.rmtree("sandboxstore", ignore_errors=True)


# -----------------------------------------------------------------------------
# Job tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file, inputs",
    [
        # --- Hello World example ---
        # There is no input expected
        ("test/workflows/helloworld/helloworld_basic/description.cwl", []),
        # An input is expected but not required (default value is used)
        ("test/workflows/helloworld/helloworld_with_inputs/description.cwl", []),
        # A string input is passed
        (
            "test/workflows/helloworld/helloworld_with_inputs/description.cwl",
            [
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs1.yaml"
            ],
        ),
        # Multiple string inputs are passed
        (
            "test/workflows/helloworld/helloworld_with_inputs/description.cwl",
            [
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs1.yaml",
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs2.yaml",
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs3.yaml",
            ],
        ),
        # --- Crypto example ---
        # Complete
        (
            "test/workflows/crypto/crypto_complete/description.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # Caesar only
        (
            "test/workflows/crypto/crypto_caesar/caesar.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # ROT13 only
        (
            "test/workflows/crypto/crypto_rot13/rot13.cwl",
            [],
        ),
        # Base64 only
        (
            "test/workflows/crypto/crypto_base64/base64.cwl",
            [],
        ),
        # MD5 only
        (
            "test/workflows/crypto/crypto_md5/md5.cwl",
            [],
        ),
        # --- Pi example ---
        # Complete
        (
            "test/workflows/pi/pi_complete/description.cwl",
            ["test/workflows/pi/type_dependencies/job/inputs-pi_complete.yaml"],
        ),
        # Simulate only
        ("test/workflows/pi/pi_simulate/pisimulate.cwl", []),
        # Gather only
        (
            "test/workflows/pi/pi_gather/pigather.cwl",
            ["test/workflows/pi/type_dependencies/job/inputs-pi_gather.yaml"],
        ),
        # --- Mandelbrot example ---
        # Complete
        (
            "test/workflows/mandelbrot/mandelbrot_complete/description.cwl",
            [
                "test/workflows/mandelbrot/type_dependencies/job/inputs-mandelbrot_complete.yaml"
            ],
        ),
        # Image production only
        ("test/workflows/mandelbrot/mandelbrot_imageprod/image-prod.cwl", []),
        # Image merge only
        (
            "test/workflows/mandelbrot/mandelbrot_imagemerge/image-merge.cwl",
            [
                "test/workflows/mandelbrot/type_dependencies/job/inputs-mandelbrot_imagemerge.yaml"
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
    assert "Job(s) done" in result.stdout, f"Failed to run the job: {result.stdout}"


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
    ],
)
def test_run_job_validation_failure(
    cli_runner, cleanup, cwl_file, inputs, expected_error
):
    command = ["job", "submit", cwl_file]
    for input in inputs:
        command.extend(["--parameter-path", input])
    result = cli_runner.invoke(app, command)
    assert "Job(s) done" not in result.stdout, "The job did complete successfully."
    assert expected_error in re.sub(
        r"\s+", "", result.stdout
    ), "The expected error was not found."


# -----------------------------------------------------------------------------
# Transformation tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file, metadata",
    [
        # --- Hello World example ---
        # There is no input expected
        ("test/workflows/helloworld/helloworld_basic/description.cwl", None),
        # --- Crypto example ---
        # Complete
        ("test/workflows/crypto/crypto_complete/description.cwl", None),
        # Caesar only
        ("test/workflows/crypto/crypto_caesar/caesar.cwl", None),
        # ROT13 only
        ("test/workflows/crypto/crypto_rot13/rot13.cwl", None),
        # Base64 only
        ("test/workflows/crypto/crypto_base64/base64.cwl", None),
        # MD5 only
        ("test/workflows/crypto/crypto_md5/md5.cwl", None),
        # --- Pi example ---
        # There is no input expected
        (
            "test/workflows/pi/pi_simulate/pisimulate.cwl",
            "test/workflows/pi/type_dependencies/transformation/metadata-pi_simulate.yaml",
        ),
        # --- Mandelbrot example ---
        (
            "test/workflows/mandelbrot/mandelbrot_imageprod/image-prod.cwl",
            "test/workflows/mandelbrot/type_dependencies/transformation/metadata-mandelbrot_imageprod.yaml",
        ),
    ],
)
def test_run_simple_transformation_success(cli_runner, cleanup, cwl_file, metadata):
    # CWL file is the first argument
    command = ["transformation", "submit", cwl_file]
    # Add the metadata file
    if metadata:
        command.extend(["--metadata-path", metadata])

    result = cli_runner.invoke(app, command)
    assert (
        "Transformation done" in result.stdout
    ), f"Failed to run the transformation: {result.stdout}"


@pytest.mark.parametrize(
    "input_transformations, cwl_file, metadata",
    [
        # --- Mandelbrot example ---
        (
            {
                "data": {
                    "task": "test/workflows/mandelbrot/mandelbrot_imageprod/image-prod.cwl",
                    "metadata": (
                        "test/workflows/mandelbrot/type_dependencies/"
                        "transformation/metadata-mandelbrot_imageprod.yaml"
                    ),
                }
            },
            "test/workflows/mandelbrot/mandelbrot_imagemerge/image-merge.cwl",
            "test/workflows/mandelbrot/type_dependencies/transformation/metadata-mandelbrot_imagemerge.yaml",
        ),
    ],
)
def test_run_blocking_transformation_success(
    cli_runner, input_transformations, cwl_file, metadata
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

    # Add the n required files to the expected input query
    for input_name, transformation_details_ in input_transformations.items():
        # Load the metadata yaml file
        with open(metadata, "r") as file:
            content = YAML(typ="safe").load(file)

        number_of_input_data = content["group_size"][input_name]
        previous_task = transformation_details_["task"]
        previous_metadata = transformation_details_["metadata"]
        for _ in range(number_of_input_data):
            command = [
                "transformation",
                "submit",
                previous_task,
                "--metadata-path",
                previous_metadata,
            ]
            cli_runner.invoke(app, command)

    # Wait for the thread to finish
    transformation_thread.join(timeout=30)

    # Check if the transformation completed successfully
    assert (
        transformation_result is not None
    ), "The transformation result was not captured."
    assert (
        "Transformation done" in transformation_result.stdout
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
    ],
)
def test_run_transformation_validation_failure(
    cli_runner, cwl_file, cleanup, metadata, expected_error
):
    command = ["transformation", "submit", cwl_file]
    if metadata:
        command.extend(["--metadata-path", metadata])
    result = cli_runner.invoke(app, command)
    assert (
        "Transformation done" not in result.stdout
    ), "The transformation did complete successfully."
    assert expected_error in re.sub(
        r"\s+", "", result.stdout
    ), "The expected error was not found."


# -----------------------------------------------------------------------------
# Production tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file, metadata",
    [
        # --- Crypto example ---
        # Complete
        ("test/workflows/crypto/crypto_complete/description.cwl", None),
        # --- Pi example ---
        # There is no input expected
        (
            "test/workflows/pi/pi_complete/description.cwl",
            "test/workflows/pi/type_dependencies/production/metadata-pi_complete.yaml",
        ),
        # --- Mandelbrot example ---
        (
            "test/workflows/mandelbrot/mandelbrot_complete/description.cwl",
            "test/workflows/mandelbrot/type_dependencies/production/metadata-mandelbrot_complete.yaml",
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
    assert (
        "Production done" in result.stdout
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
        # The workflow is a CommandLineTool instead of a Workflow
        (
            "test/workflows/helloworld/helloworld_basic/description.cwl",
            None,
            "InputshouldbeaninstanceofWorkflow",
        ),
        # The metadata has an unexistent step name
        (
            "test/workflows/mandelbrot/mandelbrot_complete/description.cwl",
            "test/workflows/mandelbrot/type_dependencies/production/malformed-wrong-stepname_metadata-mandelbrot_complete.yaml",
            "Thefollowingstepsaremissingfromthetaskworkflow:{'this-step-doesnot-exist'}",
        ),
        # The metadata has an unexistent type
        (
            "test/workflows/mandelbrot/mandelbrot_complete/description.cwl",
            "test/workflows/mandelbrot/type_dependencies/production/malformed-nonexisting-type_metadata-mandelbrot_complete.yaml",
            "Invalidtype'MandelBrotDoesNotExist'.Mustbeoneof:",
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

    assert (
        "Transformation done" not in result.stdout
    ), "The transformation did complete successfully."
    assert expected_error in re.sub(
        r"\s+", "", f"{result.stdout}"
    ) or expected_error in re.sub(
        r"\s+", "", f"{result.exception}"
    ), "The expected error was not found."

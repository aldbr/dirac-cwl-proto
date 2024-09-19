import re
import shutil
import threading
import time

import pytest
from ruamel.yaml import YAML
from typer.testing import CliRunner

from dirac_cwl_proto import app


@pytest.fixture()
def cli_runner():
    return CliRunner()


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
            ["test/workflows/helloworld/helloworld_with_inputs/inputs1.yaml"],
        ),
        # Multiple string inputs are passed
        (
            "test/workflows/helloworld/helloworld_with_inputs/description.cwl",
            [
                "test/workflows/helloworld/helloworld_with_inputs/inputs1.yaml",
                "test/workflows/helloworld/helloworld_with_inputs/inputs2.yaml",
                "test/workflows/helloworld/helloworld_with_inputs/inputs3.yaml",
            ],
        ),
        # --- Mandelbrot example ---
        (
            "test/workflows/mandelbrot/mandelbrot_complete/description.cwl",
            ["test/workflows/mandelbrot/mandelbrot_complete/inputs.yaml"],
        ),
        ("test/workflows/mandelbrot/mandelbrot_imageprod/image-prod.cwl", []),
        # TODO: should take data as input sandbox
        ("test/workflows/mandelbrot/mandelbrot_imagemerge/image-merge.cwl", []),
    ],
)
def test_run_job_success(cli_runner, cwl_file, inputs):
    shutil.rmtree("filecatalog", ignore_errors=True)

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
def test_run_job_validation_failure(cli_runner, cwl_file, inputs, expected_error):
    shutil.rmtree("filecatalog", ignore_errors=True)

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
        # --- Mandelbrot example ---
        (
            "test/workflows/mandelbrot/mandelbrot_imageprod/image-prod.cwl",
            "test/workflows/mandelbrot/mandelbrot_imageprod/metadata.yaml",
        ),
    ],
)
def test_run_simple_transformation_success(cli_runner, cwl_file, metadata):
    shutil.rmtree("filecatalog", ignore_errors=True)

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
                    "metadata": "test/workflows/mandelbrot/mandelbrot_imageprod/metadata.yaml",
                }
            },
            "test/workflows/mandelbrot/mandelbrot_imagemerge/image-merge.cwl",
            "test/workflows/mandelbrot/mandelbrot_imagemerge/metadata.yaml",
        ),
    ],
)
def test_run_blocking_transformation_success(
    cli_runner, input_transformations, cwl_file, metadata
):
    shutil.rmtree("filecatalog", ignore_errors=True)

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
    cli_runner, cwl_file, metadata, expected_error
):
    shutil.rmtree("filecatalog", ignore_errors=True)

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

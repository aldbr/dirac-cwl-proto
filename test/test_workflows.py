import re
import shutil

import pytest
from typer.testing import CliRunner

from dirac_cwl_proto import app


@pytest.fixture()
def cli_runner():
    return CliRunner()


# @pytest.mark.parametrize(
#     "cwl_file, inputs, metadata_model",
#     [
#         (
#             "workflows/generic_workflow/description.cwl",
#             "workflows/generic_workflow/inputs.yml",
#             "basic",
#         ),
#         # ("workflows/macobac_workflow/description.cwl", "workflows/macobac_workflow/inputs.yml", "macobac"),
#         (
#             "workflows/mandelbrot_workflow/description.cwl",
#             "workflows/mandelbrot_workflow/inputs.yml",
#             "mandelbrot",
#         ),
#     ],
# )
# def test_run_production_success(cli_runner, cwl_file, inputs, metadata_model):
#     shutil.rmtree("bookkeeping", ignore_errors=True)

#     result = cli_runner.invoke(
#         app, ["production", "submit", cwl_file, inputs, metadata_model]
#     )
#     assert "Production done" in result.stdout


@pytest.mark.parametrize(
    "cwl_file, inputs",
    [
        # There is no input expected
        ("test/workflows/helloworld/description.cwl", []),
        # An input is expected but not required (default value is used)
        ("test/workflows/helloworld_with_inputs/description.cwl", []),
        # A string input is passed
        (
            "test/workflows/helloworld_with_inputs/description.cwl",
            ["test/workflows/helloworld_with_inputs/inputs1.yaml"],
        ),
        # Multiple string inputs are passed
        (
            "test/workflows/helloworld_with_inputs/description.cwl",
            [
                "test/workflows/helloworld_with_inputs/inputs1.yaml",
                "test/workflows/helloworld_with_inputs/inputs2.yaml",
                "test/workflows/helloworld_with_inputs/inputs3.yaml",
            ],
        ),
    ],
)
def test_run_job_success(cli_runner, cwl_file, inputs):
    shutil.rmtree("bookkeeping", ignore_errors=True)

    command = ["job", "submit", cwl_file]
    for input in inputs:
        command.extend(["--parameter-path", input])
    result = cli_runner.invoke(app, command)
    assert "Job(s) done" in result.stdout


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
    shutil.rmtree("bookkeeping", ignore_errors=True)

    command = ["job", "submit", cwl_file]
    for input in inputs:
        command.extend(["--parameter-path", input])
    result = cli_runner.invoke(app, command)
    assert "Job(s) done" not in result.stdout
    assert expected_error in re.sub("\s+", "", result.stdout)

import shutil

import pytest
from typer.testing import CliRunner

from dirac_cwl_proto.cli import app


@pytest.fixture()
def cli_runner():
    return CliRunner()


@pytest.mark.parametrize(
    "cwl_file, inputs, metadata_model",
    [
        (
            "workflows/generic_workflow/description.cwl",
            "workflows/generic_workflow/inputs.yml",
            "basic",
        ),
        # ("workflows/macobac_workflow/description.cwl", "workflows/macobac_workflow/inputs.yml", "macobac"),
        (
            "workflows/mandelbrot_workflow/description.cwl",
            "workflows/mandelbrot_workflow/inputs.yml",
            "mandelbrot",
        ),
    ],
)
def test_run_success(cli_runner, cwl_file, inputs, metadata_model):
    shutil.rmtree("bookkeeping", ignore_errors=True)

    result = cli_runner.invoke(app, [cwl_file, inputs, metadata_model])
    assert "Workflow executed successfully" in result.stdout

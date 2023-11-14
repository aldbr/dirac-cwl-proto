import pytest
from typer.testing import CliRunner
from dirac_cwl_proto.cli import run_workflow


@pytest.fixture()
def cli_runner():
    return CliRunner()

def test_run_workflow_success(cli_runner):
    result = cli_runner.invoke(run_workflow, ["path/to/valid/cwl/file"])
    assert "Workflow executed successfully" in result.stdout

def test_run_workflow_failure(cli_runner):
    result = cli_runner.invoke(run_workflow, ["path/to/valid/cwl/file"])
    assert "Error in executing workflow" in result.stdout

def test_invalid_cwl_file(cli_runner):
    result = cli_runner.invoke(run_workflow, ["path/to/invalid/cwl/file"])
    assert "Invalid CWL file" in result.stdout

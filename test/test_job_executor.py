"""Tests for the job executor CLI helpers."""

import re
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest
import typer
from rich.console import Console

from dirac_cwl.job.executor.__main__ import print_workflow_visualization

REPO_ROOT = Path(__file__).resolve().parent.parent
HELLOWORLD_CWL = REPO_ROOT / "test/workflows/helloworld/description_basic.cwl"
CRYPTO_CWL = REPO_ROOT / "test/workflows/crypto/description.cwl"
MALFORMED_CLASS_CWL = REPO_ROOT / "test/workflows/malformed_description/description_malformed_class.cwl"
BAD_REFERENCE_CWL = REPO_ROOT / "test/workflows/bad_references/reference_doesnotexists.cwl"


def strip_ansi_codes(text: str) -> str:
    """Remove ANSI color codes from text."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def capture_workflow_visualization(workflow_path: Path) -> str:
    """Run print_workflow_visualization and return captured console output."""
    buffer = StringIO()
    test_console = Console(file=buffer, width=120, force_terminal=True)
    with patch("dirac_cwl.job.executor.__main__.console", test_console):
        print_workflow_visualization(workflow_path)
    return strip_ansi_codes(buffer.getvalue())


class TestPrintWorkflowVisualization:
    """Tests for print_workflow_visualization() function."""

    def test_command_line_tool_shows_metadata_only(self):
        """A CommandLineTool gets a graceful inputs/outputs-only style view."""
        output = capture_workflow_visualization(HELLOWORLD_CWL)

        assert "Could not visualize workflow" not in output, output
        assert "Workflow Visualization" in output
        assert "description_basic.cwl" in output
        assert "CWL Version: v1.2" in output
        assert "WORKFLOW GRAPH" not in output
        assert "INPUTS:" not in output
        assert "FINAL OUTPUTS:" not in output

    def test_workflow_shows_inputs_steps_and_outputs(self):
        """A workflow renders inputs, step wiring, and final outputs."""
        output = capture_workflow_visualization(CRYPTO_CWL)

        assert "Could not visualize workflow" not in output, output
        assert "Workflow Visualization" in output
        assert "description.cwl" in output
        assert "CWL Version: v1.2" in output
        assert "cryptographic transformations" in output

        assert "INPUTS:" in output
        assert "input_string: string" in output
        assert "shift_value: int" in output

        assert "WORKFLOW GRAPH:" in output
        for step in ("caesar_step", "base64_step", "md5_step", "rot13_step"):
            assert step in output
        assert "input_string ← input_string" in output
        assert "shift_value ← shift_value" in output

        assert "FINAL OUTPUTS:" in output
        assert "caesar_output: File ← caesar_step/output" in output
        assert "base64_output: File ← base64_step/output" in output
        assert "md5_output: File ← md5_step/output" in output
        assert "rot13_output: File ← rot13_step/output" in output

    @pytest.mark.parametrize(
        "workflow_path",
        [
            MALFORMED_CLASS_CWL,
            BAD_REFERENCE_CWL,
            REPO_ROOT / "test/workflows/does_not_exist.cwl",
        ],
    )
    def test_invalid_workflow_exits_with_error(self, workflow_path: Path):
        """Invalid or missing workflows fail during load with a clear message."""
        expected_message = "Could not load workflow"
        buffer = StringIO()
        test_console = Console(file=buffer, width=120, force_terminal=True)

        with patch("dirac_cwl.job.executor.__main__.console", test_console):
            with pytest.raises(typer.Exit) as exc_info:
                print_workflow_visualization(workflow_path)

        assert exc_info.value.exit_code == 1
        output = strip_ansi_codes(buffer.getvalue())
        assert expected_message in output

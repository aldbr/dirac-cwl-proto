"""
Tests for the LHCb experiment metadata plugins.

This module tests the LHCb-specific metadata plugins including simulation,
reconstruction, and analysis metadata implementations.
"""

from pathlib import Path
from unittest.mock import patch

from dirac_cwl_proto.metadata.plugins.lhcb import (
    LHCbAnalysisMetadata,
    LHCbMetadata,
    LHCbReconstructionMetadata,
    LHCbSimulationMetadata,
)


class TestLHCbMetadata:
    """Test the base LHCbMetadata class."""

    def test_creation(self):
        """Test LHCbMetadata creation."""
        metadata = LHCbMetadata(task_id=123, run_id=456)
        assert metadata.task_id == 123
        assert metadata.run_id == 456
        assert metadata.vo == "lhcb"

    def test_creation_with_parameters(self):
        """Test LHCbMetadata creation with custom parameters."""
        metadata = LHCbMetadata(task_id=123, run_id=456)
        assert metadata.task_id == 123
        assert metadata.run_id == 456

    def test_lhcb_path_generation(self):
        """Test LHCb-specific path generation."""
        metadata = LHCbMetadata(task_id=12345, run_id=1)

        # Test get_lhcb_base_path method
        path = metadata.get_lhcb_base_path()
        expected = Path("filecatalog/lhcb/12345/1")
        assert path == expected

    def test_get_input_query(self):
        """Test base get_input_query implementation."""
        metadata = LHCbMetadata(task_id=1, run_id=1)

        result = metadata.get_input_query("input_file")
        # Base LHCbMetadata returns None - only derived classes implement queries
        assert result is None

    def test_get_output_query(self):
        """Test base get_output_query implementation."""
        metadata = LHCbMetadata(task_id=2, run_id=2)

        result = metadata.get_output_query("output_file")
        # Base LHCbMetadata returns None - only derived classes implement queries
        assert result is None


class TestLHCbSimulationMetadata:
    """Test the LHCbSimulationMetadata class."""

    def test_creation(self):
        """Test LHCbSimulationMetadata creation."""
        metadata = LHCbSimulationMetadata(task_id=123, run_id=1)
        assert metadata.get_metadata_class() == "LHCbSimulation"
        assert "LHCb simulation" in metadata.description

    def test_creation_with_simulation_parameters(self):
        """Test creation with simulation-specific parameters."""
        metadata = LHCbSimulationMetadata(
            task_id=123,
            run_id=1,
            detector_conditions="sim-20230101-vc-md100",
            beam_energy=6500.0,
            generator_config="Pythia8",
        )
        assert metadata.detector_conditions == "sim-20230101-vc-md100"
        assert metadata.beam_energy == 6500.0
        assert metadata.generator_config == "Pythia8"

    def test_pre_process_simulation(self):
        """Test simulation pre_process method."""
        metadata = LHCbSimulationMetadata(task_id=123, run_id=1, generator_config="Pythia8")

        command = ["lhcb-simulation", "workflow.cwl"]
        result = metadata.pre_process(Path("/tmp/job"), command)

        # The pre_process method calculates optimal events and updates parameters
        # It returns the original command unchanged
        assert isinstance(result, list)
        assert result[0] == "lhcb-simulation"
        assert result[1] == "workflow.cwl"

    @patch("dirac_cwl_proto.metadata.plugins.lhcb.load_inputfile")
    @patch("builtins.open")
    @patch("dirac_cwl_proto.metadata.plugins.lhcb.YAML")
    def test_update_job_parameters(self, mock_yaml, mock_open, mock_load_inputfile):
        """Test job parameter updates."""
        metadata = LHCbSimulationMetadata(task_id=123, run_id=1)

        # Mock parameter file loading
        mock_load_inputfile.return_value = {"n_events": 1500, "generator": "Pythia8"}

        job_path = Path("/tmp/job")
        command = ["simulate.py", "--config", "params.yaml"]

        metadata._update_job_parameters(job_path, command)

        # Should have loaded parameters file
        mock_load_inputfile.assert_called_once()
        # Should have opened file for writing
        mock_open.assert_called_once()
        # Should have dumped YAML content
        mock_yaml.return_value.dump.assert_called_once()

    @patch("dirac_cwl_proto.metadata.plugins.lhcb.load_inputfile")
    @patch("builtins.open")
    @patch("dirac_cwl_proto.metadata.plugins.lhcb.YAML")
    def test_update_job_parameters_file_error(self, mock_yaml, mock_open, mock_load_inputfile):
        """Test job parameter update with file error."""
        metadata = LHCbSimulationMetadata(task_id=123, run_id=1)

        # Mock file loading error
        mock_load_inputfile.side_effect = Exception("File not found")

        job_path = Path("/tmp/job")
        command = ["simulate.py", "--config", "params.yaml"]

        # Should handle error gracefully
        metadata._update_job_parameters(job_path, command)

        # Should still try to open file for writing
        mock_open.assert_called_once()
        # Should still dump YAML content with fallback parameters
        mock_yaml.return_value.dump.assert_called_once()

    def test_pre_process(self):
        """Test simulation pre_process method."""
        metadata = LHCbSimulationMetadata(task_id=123, run_id=1, generator_config="Pythia8")

        command = ["lhcb-simulation", "workflow.cwl"]
        result = metadata.pre_process(Path("/tmp/job"), command)

        # The pre_process method calculates optimal events and updates parameters
        # It returns the original command unchanged (actual implementation doesn't modify command)
        assert isinstance(result, list)
        assert result[0] == "lhcb-simulation"
        assert result[1] == "workflow.cwl"
        # The number of events is calculated and stored in the metadata
        assert metadata.number_of_events >= 0  # Should have calculated events

    @patch("dirac_cwl_proto.metadata.plugins.lhcb.glob.glob")
    @patch("dirac_cwl_proto.metadata.core.DataCatalogInterface.store_output")
    def test_post_process(self, mock_store, mock_glob):
        """Test simulation post_process method."""
        metadata = LHCbSimulationMetadata(task_id=123, run_id=1)

        job_path = Path("/tmp/job")

        # Mock glob to find simulation files
        mock_glob.side_effect = [
            ["/tmp/job/output.sim"],  # sim files
            ["/tmp/job/pool_xml_catalog.xml"],  # catalog files
        ]

        result = metadata.post_process(job_path)

        assert result is True
        # Should call store_output for sim and catalog files
        assert mock_store.call_count == 2


class TestLHCbReconstructionMetadata:
    """Test the LHCbReconstructionMetadata class."""

    def test_creation(self):
        """Test LHCbReconstructionMetadata creation."""
        metadata = LHCbReconstructionMetadata(task_id=456, run_id=1)
        assert metadata.get_metadata_class() == "LHCbReconstruction"
        assert "LHCb reconstruction" in metadata.description

    def test_creation_with_reconstruction_parameters(self):
        """Test creation with reconstruction-specific parameters."""
        metadata = LHCbReconstructionMetadata(
            task_id=456,
            run_id=1,
            input_data_type="SIM",
            output_data_type="DST",
            reconstruction_version="v50r1",
        )
        assert metadata.input_data_type == "SIM"
        assert metadata.output_data_type == "DST"
        assert metadata.reconstruction_version == "v50r1"

    def test_validate_data_types(self):
        """Test data type validation."""
        # Valid data types
        metadata = LHCbReconstructionMetadata(task_id=456, run_id=1, input_data_type="SIM", output_data_type="DST")
        assert metadata.input_data_type == "SIM"
        assert metadata.output_data_type == "DST"

    def test_get_input_query_with_data_type(self):
        """Test input query with data type filtering."""
        metadata = LHCbReconstructionMetadata(task_id=456, run_id=1, input_data_type="SIM")

        result = metadata.get_input_query("input_files")
        expected = Path("filecatalog/lhcb/456/1/sim")
        assert result == expected

    def test_get_output_query_with_data_type(self):
        """Test output query with data type specification."""
        metadata = LHCbReconstructionMetadata(task_id=456, run_id=1, output_data_type="DST")

        result = metadata.get_output_query("output_files")
        expected = Path("filecatalog/lhcb/456/1/dst")
        assert result == expected

    def test_pre_process(self):
        """Test reconstruction pre_process method."""
        metadata = LHCbReconstructionMetadata(task_id=456, run_id=1, reconstruction_version="v50r1")

        command = ["lhcb-reconstruction", "--input", "sim.dst"]
        result = metadata.pre_process(Path("/tmp/job"), command)

        # Should add reconstruction-specific parameters
        assert "--version" in result
        assert "v50r1" in result
        assert "--input-type" in result
        assert "RAW" in result  # default input_data_type
        assert "--output-type" in result
        assert "DST" in result  # default output_data_type


class TestLHCbAnalysisMetadata:
    """Test the LHCbAnalysisMetadata class."""

    def test_creation(self):
        """Test LHCbAnalysisMetadata creation."""
        metadata = LHCbAnalysisMetadata(task_id=789, run_id=1, analysis_name="TestAnalysis", user_name="testuser")
        assert metadata.get_metadata_class() == "LHCbAnalysis"
        assert "LHCb analysis" in metadata.description

    def test_creation_with_analysis_parameters(self):
        """Test creation with analysis-specific parameters."""
        metadata = LHCbAnalysisMetadata(
            task_id=789,
            run_id=1,
            analysis_name="B2KstarMuMu",
            user_name="alice",
            analysis_version="v1.0",
        )
        assert metadata.analysis_name == "B2KstarMuMu"
        assert metadata.user_name == "alice"
        assert metadata.analysis_version == "v1.0"

    def test_user_path_generation(self):
        """Test user-specific path generation."""
        metadata = LHCbAnalysisMetadata(task_id=789, run_id=1, user_name="alice", analysis_name="B2KstarMuMu")

        result = metadata.get_input_query("input_data")
        expected = Path("filecatalog/lhcb/analysis/alice/B2KstarMuMu/input")
        assert result == expected

    def test_get_output_query_with_analysis(self):
        """Test output query with analysis-specific paths."""
        metadata = LHCbAnalysisMetadata(task_id=789, run_id=1, user_name="alice", analysis_name="B2KstarMuMu")

        result = metadata.get_output_query("results")
        expected = Path("filecatalog/lhcb/analysis/alice/B2KstarMuMu/results")
        assert result == expected

    def test_pre_process(self):
        """Test analysis pre_process method."""
        metadata = LHCbAnalysisMetadata(task_id=789, run_id=1, analysis_name="B2KstarMuMu", user_name="alice")

        command = ["python", "analysis.py"]
        result = metadata.pre_process(Path("/tmp/job"), command)

        # Should add analysis-specific parameters
        assert "--analysis" in result
        assert "B2KstarMuMu" in result
        assert "--user" in result
        assert "alice" in result

    @patch("dirac_cwl_proto.metadata.plugins.lhcb.glob.glob")
    @patch("dirac_cwl_proto.metadata.core.DataCatalogInterface.store_output")
    def test_post_process_with_user_output(self, mock_store, mock_glob):
        """Test analysis post_process with user-specific output handling."""
        metadata = LHCbAnalysisMetadata(task_id=789, run_id=1, user_name="alice", analysis_name="TestAnalysis")

        job_path = Path("/tmp/job")

        # Mock glob to find ROOT files and plot files
        mock_glob.side_effect = [
            ["/tmp/job/results.root"],  # ROOT files
            [],  # PNG files
            [],  # PDF files
            [],  # EPS files
            [],  # SVG files
        ]

        result = metadata.post_process(job_path)

        # The method should succeed
        assert result is True
        # Should call store_output for ROOT files
        mock_store.assert_called()


class TestLHCbPluginIntegration:
    """Test integration between LHCb plugins."""

    def test_all_lhcb_plugins_have_vo_namespace(self):
        """Test that all LHCb plugins have the correct VO namespace."""
        plugins = [LHCbMetadata, LHCbSimulationMetadata, LHCbReconstructionMetadata, LHCbAnalysisMetadata]

        for plugin_class in plugins:
            assert hasattr(plugin_class, "vo")
            assert plugin_class.vo == "lhcb"

    def test_lhcb_plugins_inheritance(self):
        """Test that all LHCb plugins inherit from LHCbMetadata."""
        plugins = [LHCbSimulationMetadata, LHCbReconstructionMetadata, LHCbAnalysisMetadata]

        for plugin_class in plugins:
            assert issubclass(plugin_class, LHCbMetadata)

    def test_lhcb_path_consistency(self):
        """Test that all LHCb plugins generate consistent paths."""
        plugins = [
            ("simulation", LHCbSimulationMetadata(task_id=1, run_id=1)),
            ("reconstruction", LHCbReconstructionMetadata(task_id=2, run_id=2)),
            ("analysis", LHCbAnalysisMetadata(task_id=3, run_id=3, analysis_name="test", user_name="test")),
        ]

        for _plugin_type, plugin in plugins:
            input_path = plugin.get_input_query("test_input")
            output_path = plugin.get_output_query("test_output")

            # All paths should exist (not None for these plugins)
            assert input_path is not None
            assert output_path is not None

            # All paths should contain "filecatalog/lhcb"
            assert str(input_path).startswith("filecatalog/lhcb")
            assert str(output_path).startswith("filecatalog/lhcb")

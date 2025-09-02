"""
Integration tests for the complete metadata plugin system.

This module tests the end-to-end functionality of the metadata plugin system,
including plugin discovery, registration, CWL integration, and real-world
usage scenarios.
"""

from pathlib import Path

import pytest

from dirac_cwl_proto.metadata import (
    get_registry,
)
from dirac_cwl_proto.metadata.core import DataManager, TaskRuntimeBasePlugin
from dirac_cwl_proto.submission_models import (
    TaskDescriptionModel,
)


class TestSystemIntegration:
    """Test complete system integration."""

    def test_plugin_system_initialization(self):
        """Test that the plugin system initializes correctly."""
        # Test registry access
        registry = get_registry()
        assert registry is not None

        # Test that core plugins are registered
        registry = get_registry()
        plugins = registry.list_plugins()
        core_plugins = {"User", "Admin", "QueryBased"}
        assert core_plugins.issubset(set(plugins))

    def test_plugin_instantiation_flow(self):
        """Test the complete plugin instantiation flow."""
        # Test each core plugin
        test_cases = [
            ("User", {}),
            ("Admin", {"admin_level": 5, "log_level": "DEBUG"}),
            ("QueryBased", {"query_root": "/data", "campaign": "Test"}),
        ]

        for plugin_type, params in test_cases:
            # Test direct instantiation
            registry = get_registry()
            descriptor = DataManager(metadata_class=plugin_type, **params)
            instance = registry.instantiate_plugin(descriptor)
            assert instance.get_metadata_class() == plugin_type

            # Test via descriptor
            descriptor = DataManager(metadata_class=plugin_type, query_params=params)
            runtime = descriptor.to_runtime()
            assert runtime.get_metadata_class() == plugin_type

    def test_vo_plugin_support(self):
        """Test VO-specific plugin functionality."""
        registry = get_registry()
        vos = registry.list_virtual_organizations()

        # Should have at least LHCb VO
        assert "lhcb" in vos

        # Test LHCb plugin access
        lhcb_plugins = registry.list_plugins(vo="lhcb")
        assert len(lhcb_plugins) > 0

    def test_cwl_integration_workflow(self):
        """Test complete CWL integration workflow."""
        # Create an enhanced descriptor directly to test CWL integration
        metadata_descriptor = DataManager(
            metadata_class="QueryBased",
            query_params={"campaign": "Run3", "data_type": "AOD", "site": "CERN"},
        )

        # Convert to runtime
        runtime_metadata = metadata_descriptor.to_runtime()
        assert (
            runtime_metadata.get_metadata_class() == "QueryBased"
        )  # Test that CWL parameters are available
        # (Note: exact parameter extraction depends on implementation)

    def test_legacy_compatibility_integration(self):
        """Test that legacy metadata models integrate correctly."""
        # Test that legacy models are still accessible
        legacy_plugins = [
            "PiSimulate",
            "PiGather",
            "LHCbSimulation",
            "MandelBrotGeneration",
            "GaussianFit",
        ]

        registry = get_registry()
        registered = registry.list_plugins()
        for plugin in legacy_plugins:
            assert plugin in registered


class TestRealWorldScenarios:
    """Test real-world usage scenarios."""

    def test_user_workflow_scenario(self):
        """Test a typical user workflow scenario."""
        # User creates a basic job with user metadata
        user_descriptor = DataManager(metadata_class="User")
        user_runtime = user_descriptor.to_runtime()

        # Simulate job execution
        job_path = Path("/tmp/user_job")
        command = ["python", "user_script.py"]

        # Pre-process
        processed_command = user_runtime.pre_process(job_path, command)
        assert processed_command == command  # User metadata doesn't modify command

        # Post-process
        result = user_runtime.post_process(job_path)
        assert result is True

    def test_admin_workflow_scenario(self):
        """Test an administrative workflow scenario."""
        # Admin creates a job with enhanced logging
        admin_descriptor = DataManager(
            metadata_class="Admin",
            query_params={
                "admin_level": 8,
                "log_level": "DEBUG",
                "enable_monitoring": True,
            },
        )
        admin_runtime = admin_descriptor.to_runtime()

        # Simulate job execution
        job_path = Path("/tmp/admin_job")
        command = ["python", "admin_script.py"]

        # Pre-process should add logging
        processed_command = admin_runtime.pre_process(job_path, command)
        assert len(processed_command) >= len(
            command
        )  # Should be at least the same length
        assert "--log-level" in processed_command
        assert "DEBUG" in processed_command

    def test_data_analysis_workflow_scenario(self):
        """Test a data analysis workflow scenario."""
        # Analyst creates a job with query-based data discovery
        analysis_descriptor = DataManager(
            metadata_class="QueryBased",
            query_params={
                "query_root": "/grid/data",
                "campaign": "Run3_2024",
                "data_type": "AOD",
                "site": "CERN",
            },
        )
        analysis_runtime = analysis_descriptor.to_runtime()

        # Test input data discovery
        input_path = analysis_runtime.get_input_query("input_data")
        assert str(input_path).startswith("/grid/data")
        assert "Run3_2024" in str(input_path)
        assert "CERN" in str(input_path)
        assert "AOD" in str(input_path)

        # Test output path generation
        output_path = analysis_runtime.get_output_query("results")
        assert output_path is not None

    def test_lhcb_simulation_workflow_scenario(self):
        """Test an LHCb simulation workflow scenario."""
        # Test if LHCb simulation plugin is available
        lhcb_descriptor = DataManager(
            metadata_class="LHCbSimulation",
            query_params={
                "task_id": 123,
                "run_id": 2,
                "generator": "Pythia8",
                "n_events": 10000,
            },
        )
        lhcb_runtime = lhcb_descriptor.to_runtime()

        # Test LHCb-specific functionality
        assert lhcb_runtime.get_metadata_class() == "LHCbSimulation"

        # Test path generation
        input_path = lhcb_runtime.get_input_query("gen_file")
        output_path = lhcb_runtime.get_output_query("sim_file")

        assert "lhcb" in str(input_path).lower()
        assert "lhcb" in str(output_path).lower()

    def test_transformation_workflow_scenario(self):
        """Test a transformation (batch processing) workflow scenario."""
        # Create multiple task descriptions for a transformation
        task_configs = [
            {"platform": "DIRAC", "priority": 5, "sites": [f"site_{i}"]}
            for i in range(5)
        ]

        tasks = []
        for config in task_configs:
            task = TaskDescriptionModel(**config)
            tasks.append(task)

        # Verify all tasks have correct configuration
        for i, task in enumerate(tasks):
            assert task.platform == "DIRAC"
            assert task.priority == 5
            assert task.sites == [f"site_{i}"]

    def test_parameter_override_scenario(self):
        """Test parameter override scenarios."""
        # Base descriptor with default parameters
        base_descriptor = DataManager(
            metadata_class="Admin", query_params={"admin_level": 3, "log_level": "INFO"}
        )

        # Create variants with different overrides
        variants = [
            {"admin_level": 5},
            {"log_level": "DEBUG"},
            {"admin_level": 8, "log_level": "ERROR", "enable_monitoring": False},
        ]

        for override in variants:
            variant_descriptor = base_descriptor.model_copy(
                update={"query_params": override}
            )
            runtime = variant_descriptor.to_runtime()

            # Verify parameters are properly merged/overridden
            for key, value in override.items():
                assert getattr(runtime, key) == value


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_invalid_plugin_type(self):
        """Test handling of invalid plugin types."""
        # Invalid types should raise error during runtime instantiation
        descriptor = DataManager(metadata_class="NonExistentPlugin")
        with pytest.raises(KeyError, match="Unknown metadata plugin"):
            descriptor.to_runtime()

    def test_missing_required_parameters(self):
        """Test handling of missing required parameters."""
        # Some plugins might require specific parameters
        with pytest.raises(
            ValueError, match="Failed to instantiate plugin 'LHCbSimulation'"
        ):
            descriptor = DataManager(metadata_class="LHCbSimulation")
            descriptor.to_runtime()

    def test_plugin_registration_conflicts(self):
        """Test handling of plugin registration conflicts."""

        # Create a test plugin
        class ConflictTestPlugin(TaskRuntimeBasePlugin):
            description = "Test plugin for conflict testing"

        # Register it
        registry = get_registry()
        registry.register_plugin(ConflictTestPlugin)

        # Try to register again (should raise error)
        with pytest.raises(ValueError, match="already registered"):
            registry.register_plugin(ConflictTestPlugin)

    def test_malformed_cwl_hints(self, mocker):
        """Test handling of malformed CWL hints."""
        # Test with None hints
        mock_cwl = mocker.Mock()
        mock_cwl.hints = None

        descriptor = DataManager.from_cwl(mock_cwl)
        assert descriptor.metadata_class == "User"  # Should use default

        # Test with empty hints
        mock_cwl.hints = []
        descriptor = DataManager.from_cwl(mock_cwl)
        assert descriptor.metadata_class == "User"  # Should use default

        # Test with malformed hints
        mock_cwl.hints = [{"invalid": "hint"}]
        descriptor = DataManager.from_cwl(mock_cwl)
        assert descriptor.metadata_class == "User"  # Should ignore and use default


class TestPerformance:
    """Test performance-related aspects."""

    def test_plugin_instantiation_performance(self):
        """Test that plugin instantiation is reasonably fast."""
        import time

        # Test instantiation of multiple plugins
        start_time = time.time()

        for _ in range(100):
            descriptor = DataManager(metadata_class="User")
            descriptor.to_runtime()

        end_time = time.time()

        # Should complete in reasonable time (adjust threshold as needed)
        assert (end_time - start_time) < 1.0  # 1 second for 100 instantiations

    def test_registry_lookup_performance(self):
        """Test that registry lookups are efficient."""
        import time

        registry = get_registry()

        start_time = time.time()

        for _ in range(1000):
            registry.list_plugins()
            registry.get_plugin("User")

        end_time = time.time()

        # Registry operations should be fast
        assert (end_time - start_time) < 0.5  # 0.5 seconds for 1000 operations

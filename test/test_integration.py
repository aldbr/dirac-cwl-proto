"""
Integration tests for the complete metadata plugin system.

This module tests the end-to-end functionality of the metadata plugin system,
including plugin discovery, registration, CWL integration, and real-world
usage scenarios.
"""

from pathlib import Path

import pytest

from dirac_cwl_proto.execution_hooks import (
    get_registry,
)
from dirac_cwl_proto.execution_hooks.core import (
    ExecutionHooksBasePlugin,
    ExecutionHooksHint,
    SchedulingHint,
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
        core_plugins = {"UserPlugin", "AdminPlugin", "QueryBasedPlugin"}
        assert core_plugins.issubset(set(plugins))

    def test_plugin_instantiation_flow(self):
        """Test the complete plugin instantiation flow."""
        # Test each core plugin
        test_cases = [
            ("UserPlugin", {}),
            ("AdminPlugin", {"admin_level": 5, "log_level": "DEBUG"}),
            ("QueryBasedPlugin", {"query_root": "/data", "campaign": "Test"}),
        ]

        for plugin_type, params in test_cases:
            # Test direct instantiation
            registry = get_registry()
            descriptor = ExecutionHooksHint(hook_plugin=plugin_type, **params)
            instance = registry.instantiate_plugin(descriptor)
            assert instance.name() == plugin_type

            # Test via descriptor
            descriptor = ExecutionHooksHint(
                hook_plugin=plugin_type, configuration=params
            )
            runtime = descriptor.to_runtime()
            assert runtime.name() == plugin_type

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
        metadata_descriptor = ExecutionHooksHint(
            hook_plugin="QueryBasedPlugin",
            configuration={"campaign": "Run3", "data_type": "AOD", "site": "CERN"},
        )

        # Convert to runtime
        runtime_metadata = metadata_descriptor.to_runtime()
        assert (
            runtime_metadata.name() == "QueryBasedPlugin"
        )  # Test that CWL parameters are available
        # (Note: exact parameter extraction depends on implementation)


class TestRealWorldScenarios:
    """Test real-world usage scenarios."""

    def test_user_workflow_scenario(self):
        """Test a typical user workflow scenario."""
        # User creates a basic job with user metadata
        user_descriptor = ExecutionHooksHint(hook_plugin="UserPlugin")
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
        admin_descriptor = ExecutionHooksHint(
            hook_plugin="AdminPlugin",
            configuration={
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
        analysis_descriptor = ExecutionHooksHint(
            hook_plugin="QueryBasedPlugin",
            configuration={
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
        lhcb_descriptor = ExecutionHooksHint(
            hook_plugin="LHCbSimulationPlugin",
            configuration={
                "task_id": 123,
                "run_id": 2,
                "number_of_events": 10000,
            },
        )
        lhcb_runtime = lhcb_descriptor.to_runtime()

        # Test LHCb-specific functionality
        assert lhcb_runtime.name() == "LHCbSimulationPlugin"

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
            task = SchedulingHint(**config)
            tasks.append(task)

        # Verify all tasks have correct configuration
        for i, task in enumerate(tasks):
            assert task.platform == "DIRAC"
            assert task.priority == 5
            assert task.sites == [f"site_{i}"]

    def test_parameter_override_scenario(self):
        """Test parameter override scenarios."""
        # Base descriptor with default parameters
        base_descriptor = ExecutionHooksHint(
            hook_plugin="AdminPlugin",
            configuration={"admin_level": 3, "log_level": "INFO"},
        )

        # Create variants with different overrides
        variants = [
            {"admin_level": 5},
            {"log_level": "DEBUG"},
            {"admin_level": 8, "log_level": "ERROR", "enable_monitoring": False},
        ]

        for override in variants:
            variant_descriptor = base_descriptor.model_copy(
                update={"configuration": override}
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
        descriptor = ExecutionHooksHint(hook_plugin="NonExistentPlugin")
        with pytest.raises(KeyError, match="Unknown metadata plugin"):
            descriptor.to_runtime()

    def test_missing_required_parameters(self):
        """Test handling of missing required parameters."""
        # Some plugins might require specific parameters
        with pytest.raises(
            ValueError, match="Failed to instantiate plugin 'LHCbSimulationPlugin'"
        ):
            descriptor = ExecutionHooksHint(hook_plugin="LHCbSimulationPlugin")
            descriptor.to_runtime()

    def test_plugin_registration_conflicts(self):
        """Test handling of plugin registration conflicts."""

        # Create a test plugin
        class ConflictTestPlugin(ExecutionHooksBasePlugin):
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

        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)
        assert descriptor.hook_plugin == "UserPlugin"  # Should use default

        # Test with empty hints
        mock_cwl.hints = []
        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)
        assert descriptor.hook_plugin == "UserPlugin"  # Should use default

        # Test with malformed hints
        mock_cwl.hints = [{"invalid": "hint"}]
        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)
        assert descriptor.hook_plugin == "UserPlugin"  # Should ignore and use default

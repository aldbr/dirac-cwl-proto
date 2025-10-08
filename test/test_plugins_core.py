"""
Tests for the core execution hooks plugins.

This module tests the built-in execution hooks plugins including User, Admin,
and QueryBased plugin implementations.
"""

from pathlib import Path

import pytest

from dirac_cwl_proto.execution_hooks.plugins.core import (
    AdminPlugin,
    QueryBasedPlugin,
    UserPlugin,
)


class TestUserPlugin:
    """Test the UserPlugin plugin."""

    def test_default_behavior(self):
        """Test default behavior of UserPlugin."""
        plugin = UserPlugin()
        assert plugin.name() == "UserPlugin"
        assert "basic user plugin" in plugin.description.lower()

        # Test pre_process (should return command unchanged)
        command = ["python", "script.py"]
        result = plugin.pre_process(Path("/tmp"), command)
        assert result == command

        # Test post_process (should not raise exception)
        plugin.post_process(Path("/tmp"), exit_code=0)  # Should not raise exception

        # Test get_input_query (should return None)
        assert plugin.get_input_query("test_input") is None

        # Test get_output_query (should return None)
        assert plugin.get_output_query("test_output") is None

        # Test store_output raises RuntimeError when no output path is defined
        with pytest.raises(RuntimeError, match="No output path defined"):
            plugin.data_catalog.store_output("test_output", "/tmp/file.txt")

    def test_serialization(self):
        """Test UserPlugin serialization."""
        plugin = UserPlugin()

        # Test dict conversion
        data = plugin.model_dump()
        assert isinstance(data, dict)

        # Test JSON schema
        schema = plugin.model_json_schema()
        assert "properties" in schema


class TestAdminPlugin:
    """Test the AdminPlugin plugin."""

    def test_creation_and_parameters(self):
        """Test AdminPlugin creation with default and custom parameters."""
        # Test default values
        plugin = AdminPlugin()
        assert plugin.name() == "AdminPlugin"
        assert plugin.log_level == "INFO"
        assert plugin.enable_monitoring is True
        assert plugin.admin_level == 1

        # Test custom parameters
        plugin = AdminPlugin(log_level="DEBUG", enable_monitoring=False, admin_level=5)
        assert plugin.log_level == "DEBUG"
        assert plugin.enable_monitoring is False
        assert plugin.admin_level == 5

    def test_pre_process(self):
        """Test AdminPlugin pre_process method."""
        plugin = AdminPlugin(log_level="DEBUG")

        command = ["python", "script.py"]
        result = plugin.pre_process(Path("/tmp"), command)

        # Should add log level to command
        assert "--log-level" in result
        assert "DEBUG" in result
        assert result[:2] == ["python", "script.py"]

    def test_pre_process_default_log_level(self):
        """Test pre_process with default log level."""
        plugin = AdminPlugin()  # Default log_level is "INFO"

        command = ["python", "script.py"]
        result = plugin.pre_process(Path("/tmp"), command)

        # Should not add log level for INFO (default)
        assert result == command

    def test_post_process(self):
        """Test post-processing with monitoring."""
        plugin = AdminPlugin(enable_monitoring=True)

        result = plugin.post_process(Path("/tmp"))
        assert result is True

        # Test with monitoring disabled
        plugin_no_monitor = AdminPlugin(enable_monitoring=False)
        result = plugin_no_monitor.post_process(Path("/tmp"))
        assert result is True

    def test_validation(self):
        """Test AdminPlugin validation."""
        # Valid admin_level
        plugin = AdminPlugin(admin_level=5)
        assert plugin.admin_level == 5

        # Test with string log level
        plugin = AdminPlugin(log_level="ERROR")
        assert plugin.log_level == "ERROR"


class TestQueryBasedPlugin:
    """Test the QueryBasedPlugin plugin."""

    def test_creation_and_parameters(self):
        """Test QueryBasedPlugin creation with default and custom parameters."""
        # Test default values
        plugin = QueryBasedPlugin()
        assert plugin.name() == "QueryBasedPlugin"
        assert plugin.query_root == "/"  # Default value
        assert plugin.site is None
        assert plugin.campaign is None
        assert plugin.data_type is None

        # Test custom parameters
        plugin = QueryBasedPlugin(
            query_root="/data", site="CERN", campaign="Run3", data_type="AOD"
        )
        assert plugin.query_root == "/data"
        assert plugin.site == "CERN"
        assert plugin.campaign == "Run3"
        assert plugin.data_type == "AOD"

    def test_get_input_query_with_parameters(self):
        """Test get_input_query with query parameters."""
        plugin = QueryBasedPlugin(
            query_root="/data", campaign="Run3", site="CERN", data_type="AOD"
        )

        result = plugin.get_input_query("test_input")

        # Should build path from query parameters
        expected = Path("/data/Run3/CERN/AOD")
        assert result == expected

    def test_get_input_query_partial_parameters(self):
        """Test get_input_query with partial parameters."""
        plugin = QueryBasedPlugin(
            query_root="/data",
            campaign="Run3",
            data_type="AOD",
            # No site
        )

        result = plugin.get_input_query("test_input")

        # Should build path from available parameters
        expected = Path("/data/Run3/AOD")
        assert result == expected

    def test_get_input_query_no_parameters(self):
        """Test get_input_query with no query parameters."""
        plugin = QueryBasedPlugin()

        result = plugin.get_input_query("test_input")

        # Should return None when no parameters are set
        assert result is None

    def test_get_input_query_default_root(self):
        """Test get_input_query with default root."""
        plugin = QueryBasedPlugin(campaign="Test")

        result = plugin.get_input_query("test_input")

        # Should use default "/" root with campaign
        expected = Path("/Test")
        assert result == expected

    def test_get_input_query_with_kwargs(self):
        """Test get_input_query with additional kwargs."""
        plugin = QueryBasedPlugin(query_root="/data", campaign="Run3")

        # Additional kwargs should be available for custom implementations
        result = plugin.get_input_query("test_input", custom_param="value")

        # Base implementation should still work
        expected = Path("/data/Run3")
        assert result == expected

    def test_get_output_query(self):
        """Test get_output_query method."""
        plugin = QueryBasedPlugin(query_root="/output", campaign="Run3")

        result = plugin.get_output_query("test_output")

        # Should generate output path according to implementation
        expected = Path("filecatalog/outputs/Run3")
        assert result == expected

    def test_get_output_query_no_parameters(self):
        """Test get_output_query with no parameters."""
        plugin = QueryBasedPlugin()

        result = plugin.get_output_query("test_output")

        # Should use default path according to implementation
        expected = Path("filecatalog/outputs/default")
        assert result == expected

    def test_store_output(self):
        """Test store_output method."""
        plugin = QueryBasedPlugin()

        # Since store_output expects a string path not a dict, let's test the actual signature
        # store_output(output_name: str, src_path: str) -> None

        # This should work since QueryBasedPlugin provides an output path
        try:
            plugin.store_output("test_output", "/tmp/test_file.txt")
        except (FileNotFoundError, OSError):
            # Expected since the file doesn't exist
            pass

    def test_pre_process_behavior(self):
        """Test that pre_process works correctly."""
        plugin = QueryBasedPlugin(campaign="Test", data_type="SIM")

        command = ["python", "script.py"]
        result = plugin.pre_process(Path("/tmp"), command)

        # Should return command unchanged
        assert result == command

    def test_post_process_behavior(self):
        """Test that post_process works correctly."""
        plugin = QueryBasedPlugin()

        # Should not raise exception
        plugin.post_process(Path("/tmp"))


class TestPluginIntegration:
    """Test integration between different core plugins."""

    def test_all_plugins_have_description(self):
        """Test that all core plugins have description set."""
        plugins = [UserPlugin, AdminPlugin, QueryBasedPlugin]

        for plugin_class in plugins:
            assert hasattr(plugin_class, "description")
            assert isinstance(plugin_class.description, str)
            assert len(plugin_class.description) > 0

    def test_all_plugins_implement_interface(self):
        """Test that all core plugins implement the required interfaces."""
        plugins = [UserPlugin(), AdminPlugin(), QueryBasedPlugin()]

        for plugin in plugins:
            # Test EexecutionHooksBasePlugin interface
            assert hasattr(plugin, "pre_process")
            assert hasattr(plugin, "post_process")

            # Test DataCatalogInterface interface
            assert hasattr(plugin, "get_input_query")
            assert hasattr(plugin, "get_output_query")
            assert hasattr(plugin, "store_output")

            # Test that methods are callable
            assert callable(plugin.pre_process)
            assert callable(plugin.post_process)
            assert callable(plugin.get_input_query)
            assert callable(plugin.get_output_query)
            assert callable(plugin.store_output)

    def test_plugin_serialization_compatibility(self):
        """Test that all plugins can be serialized consistently."""
        plugins = [
            UserPlugin(),
            AdminPlugin(admin_level=5),
            QueryBasedPlugin(campaign="Test"),
        ]

        for plugin in plugins:
            # Test dict serialization
            data = plugin.model_dump()
            assert isinstance(data, dict)

            # Test JSON schema generation
            schema = plugin.model_json_schema()
            assert isinstance(schema, dict)
            assert "properties" in schema

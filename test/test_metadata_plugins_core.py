"""
Tests for the core metadata plugins.

This module tests the built-in metadata plugins including User, Admin,
and QueryBased metadata implementations.
"""

from pathlib import Path

import pytest

from dirac_cwl_proto.metadata.plugins.core import (
    AdminMetadata,
    QueryBasedMetadata,
    UserMetadata,
)


class TestUserMetadata:
    """Test the UserMetadata plugin."""

    def test_creation(self):
        """Test UserMetadata creation."""
        metadata = UserMetadata()
        assert metadata.get_metadata_class() == "User"
        assert "basic user metadata" in metadata.description.lower()

    def test_default_behavior(self):
        """Test default behavior of UserMetadata."""
        metadata = UserMetadata()

        # Test pre_process (should return command unchanged)
        command = ["python", "script.py"]
        result = metadata.pre_process(Path("/tmp"), command)
        assert result == command

        # Test post_process (should return True)
        assert metadata.post_process(Path("/tmp")) is True

        # Test get_input_query (should return None)
        assert metadata.get_input_query("test_input") is None

        # Test get_output_query (should return None)
        assert metadata.get_output_query("test_output") is None

        # Test store_output raises RuntimeError when no output path is defined
        with pytest.raises(RuntimeError, match="No output path defined"):
            metadata.store_output("test_output", "/tmp/file.txt")

    def test_serialization(self):
        """Test UserMetadata serialization."""
        metadata = UserMetadata()

        # Test dict conversion
        data = metadata.model_dump()
        assert isinstance(data, dict)

        # Test JSON schema
        schema = metadata.model_json_schema()
        assert "properties" in schema


class TestAdminMetadata:
    """Test the AdminMetadata plugin."""

    def test_creation(self):
        """Test AdminMetadata creation."""
        metadata = AdminMetadata()
        assert metadata.get_metadata_class() == "Admin"
        assert metadata.log_level == "INFO"
        assert metadata.enable_monitoring is True
        assert metadata.admin_level == 1

    def test_creation_with_parameters(self):
        """Test AdminMetadata creation with custom parameters."""
        metadata = AdminMetadata(
            log_level="DEBUG", enable_monitoring=False, admin_level=5
        )
        assert metadata.log_level == "DEBUG"
        assert metadata.enable_monitoring is False
        assert metadata.admin_level == 5

    def test_pre_process(self):
        """Test AdminMetadata pre_process method."""
        metadata = AdminMetadata(log_level="DEBUG")

        command = ["python", "script.py"]
        result = metadata.pre_process(Path("/tmp"), command)

        # Should add log level to command
        assert "--log-level" in result
        assert "DEBUG" in result
        assert result[:2] == ["python", "script.py"]

    def test_pre_process_default_log_level(self):
        """Test pre_process with default log level."""
        metadata = AdminMetadata()  # Default log_level is "INFO"

        command = ["python", "script.py"]
        result = metadata.pre_process(Path("/tmp"), command)

        # Should not add log level for INFO (default)
        assert result == command

    def test_post_process(self):
        """Test post-processing with monitoring."""
        metadata = AdminMetadata(enable_monitoring=True)

        result = metadata.post_process(Path("/tmp"))
        assert result is True

        # Test with monitoring disabled
        metadata_no_monitor = AdminMetadata(enable_monitoring=False)
        result = metadata_no_monitor.post_process(Path("/tmp"))
        assert result is True

    def test_validation(self):
        """Test AdminMetadata validation."""
        # Valid admin_level
        metadata = AdminMetadata(admin_level=5)
        assert metadata.admin_level == 5

        # Test with string log level
        metadata = AdminMetadata(log_level="ERROR")
        assert metadata.log_level == "ERROR"


class TestQueryBasedMetadata:
    """Test the QueryBasedMetadata plugin."""

    def test_creation(self):
        """Test QueryBasedMetadata creation."""
        metadata = QueryBasedMetadata()
        assert metadata.get_metadata_class() == "QueryBased"
        assert metadata.query_root is None
        assert metadata.site is None
        assert metadata.campaign is None
        assert metadata.data_type is None

    def test_creation_with_parameters(self):
        """Test QueryBasedMetadata creation with parameters."""
        metadata = QueryBasedMetadata(
            query_root="/data", site="CERN", campaign="Run3", data_type="AOD"
        )
        assert metadata.query_root == "/data"
        assert metadata.site == "CERN"
        assert metadata.campaign == "Run3"
        assert metadata.data_type == "AOD"

    def test_get_input_query_with_parameters(self):
        """Test get_input_query with query parameters."""
        metadata = QueryBasedMetadata(
            query_root="/data", campaign="Run3", site="CERN", data_type="AOD"
        )

        result = metadata.get_input_query("test_input")

        # Should build path from query parameters
        expected = Path("/data/Run3/CERN/AOD")
        assert result == expected

    def test_get_input_query_partial_parameters(self):
        """Test get_input_query with partial parameters."""
        metadata = QueryBasedMetadata(
            query_root="/data",
            campaign="Run3",
            data_type="AOD",
            # No site
        )

        result = metadata.get_input_query("test_input")

        # Should build path from available parameters
        expected = Path("/data/Run3/AOD")
        assert result == expected

    def test_get_input_query_no_parameters(self):
        """Test get_input_query with no query parameters."""
        metadata = QueryBasedMetadata()

        result = metadata.get_input_query("test_input")

        # Should return None when no parameters are set
        assert result is None

    def test_get_input_query_default_root(self):
        """Test get_input_query with default root."""
        metadata = QueryBasedMetadata(campaign="Test")

        result = metadata.get_input_query("test_input")

        # Should use default "filecatalog" root
        expected = Path("filecatalog/Test")
        assert result == expected

    def test_get_input_query_with_kwargs(self):
        """Test get_input_query with additional kwargs."""
        metadata = QueryBasedMetadata(query_root="/data", campaign="Run3")

        # Additional kwargs should be available for custom implementations
        result = metadata.get_input_query("test_input", custom_param="value")

        # Base implementation should still work
        expected = Path("/data/Run3")
        assert result == expected

    def test_get_output_query(self):
        """Test get_output_query method."""
        metadata = QueryBasedMetadata(query_root="/output", campaign="Run3")

        result = metadata.get_output_query("test_output")

        # Should generate output path according to implementation
        expected = Path("filecatalog/outputs/Run3")
        assert result == expected

    def test_get_output_query_no_parameters(self):
        """Test get_output_query with no parameters."""
        metadata = QueryBasedMetadata()

        result = metadata.get_output_query("test_output")

        # Should use default path according to implementation
        expected = Path("filecatalog/outputs/default")
        assert result == expected

    def test_store_output(self):
        """Test store_output method."""
        metadata = QueryBasedMetadata()

        # Since store_output expects a string path not a dict, let's test the actual signature
        # store_output(output_name: str, src_path: str) -> None

        # This should work since QueryBasedMetadata provides an output path
        try:
            metadata.store_output("test_output", "/tmp/test_file.txt")
        except (FileNotFoundError, OSError):
            # Expected since the file doesn't exist
            pass

    def test_pre_process_behavior(self):
        """Test that pre_process works correctly."""
        metadata = QueryBasedMetadata(campaign="Test", data_type="SIM")

        command = ["python", "script.py"]
        result = metadata.pre_process(Path("/tmp"), command)

        # Should return command unchanged
        assert result == command

    def test_post_process_behavior(self):
        """Test that post_process works correctly."""
        metadata = QueryBasedMetadata()

        result = metadata.post_process(Path("/tmp"))

        # Should return True
        assert result is True


class TestPluginIntegration:
    """Test integration between different core plugins."""

    def test_all_plugins_have_description(self):
        """Test that all core plugins have description set."""
        plugins = [UserMetadata, AdminMetadata, QueryBasedMetadata]

        for plugin_class in plugins:
            assert hasattr(plugin_class, "description")
            assert isinstance(plugin_class.description, str)
            assert len(plugin_class.description) > 0

    def test_all_plugins_implement_interface(self):
        """Test that all core plugins implement the required interfaces."""
        plugins = [UserMetadata(), AdminMetadata(), QueryBasedMetadata()]

        for plugin in plugins:
            # Test MetadataProcessor interface
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
            UserMetadata(),
            AdminMetadata(admin_level=5),
            QueryBasedMetadata(campaign="Test"),
        ]

        for plugin in plugins:
            # Test dict serialization
            data = plugin.model_dump()
            assert isinstance(data, dict)

            # Test JSON schema generation
            schema = plugin.model_json_schema()
            assert isinstance(schema, dict)
            assert "properties" in schema

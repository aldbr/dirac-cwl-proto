"""
Tests for the core execution hooks plugins.

This module tests the built-in execution hooks plugins including the
QueryBased plugin implementation.
"""

from pathlib import Path

import pytest

from dirac_cwl_proto.commands import PostProcessCommand, PreProcessCommand
from dirac_cwl_proto.core.exceptions import WorkflowProcessingException
from dirac_cwl_proto.execution_hooks.plugins.core import (
    QueryBasedPlugin,
)


class TestQueryBasedPlugin:
    """Test the QueryBasedPlugin plugin."""

    def test_creation_and_parameters(self):
        """Test QueryBasedPlugin creation with default and custom parameters."""
        # Test default values
        plugin = QueryBasedPlugin()
        assert plugin.name() == "QueryBasedPlugin"
        assert plugin.query_root == "/grid/data"  # Default value
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

        # Should build path from query parameters (includes campaign/site/data_type and filename)
        expected = Path("/data/Run3/CERN/AOD/test_input")
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

        # Should build path from available parameters (includes campaign/data_type and filename)
        expected = Path("/data/Run3/AOD/test_input")
        assert result == expected

    def test_get_input_query_no_parameters(self):
        """Test get_input_query with no query parameters."""
        plugin = QueryBasedPlugin()

        result = plugin.get_input_query("test_input")

        # Should return a path under the default root when no parameters are set
        expected = Path("/grid/data/test_input")
        assert result == expected

    def test_get_input_query_default_root(self):
        """Test get_input_query with default root."""
        plugin = QueryBasedPlugin(campaign="Test")

        result = plugin.get_input_query("test_input")

        # Should use default "/grid/data" root with campaign
        expected = Path("/grid/data/Test/test_input")
        assert result == expected

    def test_get_input_query_with_kwargs(self):
        """Test get_input_query with additional kwargs."""
        plugin = QueryBasedPlugin(query_root="/data", campaign="Run3")

        # Additional kwargs should be available for custom implementations
        result = plugin.get_input_query("test_input", custom_param="value")

        # Base implementation should still work (includes campaign and filename)
        expected = Path("/data/Run3/test_input")
        assert result == expected

    def test_get_output_query(self):
        """Test get_output_query method."""
        plugin = QueryBasedPlugin(query_root="/output", campaign="Run3")

        result = plugin.get_output_query("test_output")

        # Should generate output path according to DefaultDataCatalogInterface
        expected = Path("/output/outputs/Run3")
        assert result == expected

    def test_get_output_query_no_parameters(self):
        """Test get_output_query with no parameters."""
        plugin = QueryBasedPlugin()

        result = plugin.get_output_query("test_output")

        # Should use default path according to DefaultDataCatalogInterface
        expected = Path("/grid/data/outputs")
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
        plugins = [QueryBasedPlugin]

        for plugin_class in plugins:
            assert hasattr(plugin_class, "description")
            assert isinstance(plugin_class.description, str)
            assert len(plugin_class.description) > 0

    def test_all_plugins_implement_interface(self):
        """Test that all core plugins implement the required interfaces."""
        plugins = [QueryBasedPlugin()]

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


class TestPluginCommands:
    """Test plugin preprocessing and postprocessing commands."""

    @pytest.fixture
    def plugin_class_fixture(self):
        from dirac_cwl_proto.commands.download_config import DownloadConfig
        from dirac_cwl_proto.commands.group_outputs import GroupOutputs
        from dirac_cwl_proto.execution_hooks.core import ExecutionHooksBasePlugin
        from dirac_cwl_proto.execution_hooks.registry import get_registry

        registry = get_registry()
        plugin_class = registry.get_plugin("JobTypeTestingPlugin")

        if not plugin_class:

            class JobTypeTestingPlugin(ExecutionHooksBasePlugin):
                def __init__(self, **data):
                    super().__init__(**data)

                    self.preprocess_commands = [DownloadConfig]
                    self.postprocess_commands = [GroupOutputs]

            registry.register_plugin(JobTypeTestingPlugin)
            plugin_class = JobTypeTestingPlugin

        return plugin_class

    @pytest.fixture
    def command_factory_fixture(self, mocker, monkeypatch):
        def _inner(classes, raises):
            class Command(*classes):
                def execute(job_path, **kwargs):
                    return

            execute_mock = mocker.MagicMock()

            if raises:
                execute_mock.side_effect = WorkflowProcessingException()

            monkeypatch.setattr(Command, "execute", execute_mock)

            return Command, execute_mock

        return _inner

    def test_from_registry(self, plugin_class_fixture):
        """Test the initialization from the registry."""
        from dirac_cwl_proto.execution_hooks.registry import get_registry

        registry = get_registry()

        plugin_instance = registry.get_plugin("JobTypeTestingPlugin")()

        assert isinstance(plugin_instance, plugin_class_fixture)

        assert len(plugin_instance.preprocess_commands) == 1
        assert len(plugin_instance.postprocess_commands) == 1

    def test_from_hints(self, plugin_class_fixture):
        """Test the initialization from an ExecutionHooksHint."""
        from dirac_cwl_proto.execution_hooks.core import ExecutionHooksHint

        hint = ExecutionHooksHint(hook_plugin=plugin_class_fixture.__name__)
        plugin_from_hint = hint.to_runtime()

        assert isinstance(plugin_from_hint, plugin_class_fixture)

        assert len(plugin_from_hint.preprocess_commands) == 1
        assert len(plugin_from_hint.postprocess_commands) == 1

    @pytest.mark.parametrize(
        "classes, raises, exception",
        [
            # Pre-process a pre-process commamd -> Everything works as expected
            ([PreProcessCommand], False, None),
            # Pre-process a post-process commamd -> Expected a TypeError
            ([PostProcessCommand], False, TypeError),
            # Pre-process a pre-process + post-process commamd -> Everything works as expected
            ([PreProcessCommand, PostProcessCommand], False, None),
            # Pre-process a faulty pre-process command -> WorkflowProcessingException raised
            ([PreProcessCommand], True, WorkflowProcessingException),
        ],
    )
    def test_pre_process(
        self, plugin_class_fixture, command_factory_fixture, classes, raises, exception
    ):
        plugin = plugin_class_fixture()

        command_class, execute_mock = command_factory_fixture(classes, raises)
        plugin.preprocess_commands = [command_class]

        if exception:
            with pytest.raises(exception):
                plugin.pre_process("/fake/dir", ["fake", "command"])
        else:
            plugin.pre_process("/fake/dir", ["fake", "command"])
            execute_mock.assert_called_once()

    @pytest.mark.parametrize(
        "classes, raises, exception",
        [
            # Post-process a pre-process commamd -> Expected a TypeError
            ([PreProcessCommand], False, TypeError),
            # Post-process a post-process commamd -> Everything works as expected
            ([PostProcessCommand], False, None),
            # Post-process a pre-process + post-process commamd -> Everything works as expected
            ([PreProcessCommand, PostProcessCommand], False, None),
            # Post-process a faulty post-process command -> WorkflowProcessingException raised
            ([PostProcessCommand], True, WorkflowProcessingException),
        ],
    )
    def test_post_process(
        self, plugin_class_fixture, command_factory_fixture, classes, raises, exception
    ):
        plugin = plugin_class_fixture()

        command_class, execute_mock = command_factory_fixture(classes, raises)
        plugin.postprocess_commands = [command_class]

        if exception:
            with pytest.raises(exception):
                plugin.post_process("/fake/dir")
        else:
            plugin.post_process("/fake/dir")
            execute_mock.assert_called_once()

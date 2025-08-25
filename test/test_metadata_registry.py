"""
Tests for the metadata plugin registry system.

This module tests the plugin registration, discovery, and instantiation
functionality of the metadata registry.
"""

from pathlib import Path
from typing import Any, ClassVar, Optional

import pytest

from dirac_cwl_proto.metadata.core import BaseMetadataModel, MetadataDescriptor
from dirac_cwl_proto.metadata.registry import (
    MetadataPluginRegistry,
    get_registry,
    instantiate_metadata,
    list_registered,
    register_metadata,
)


class TestMetadataPlugin(BaseMetadataModel):
    """Test plugin for registry testing."""

    metadata_type: ClassVar[str] = "TestPlugin"
    description: ClassVar[str] = "Test plugin for unit tests"

    test_param: str = "default"


class TestExperimentPlugin(BaseMetadataModel):
    """Test experiment-specific plugin."""

    metadata_type: ClassVar[str] = "ExperimentTest"
    description: ClassVar[str] = "Test experiment plugin"
    experiment: ClassVar[Optional[str]] = "test_exp"

    exp_param: int = 42


class TestSecondExperimentPlugin(BaseMetadataModel):
    """Test plugin for second experiment."""

    metadata_type: ClassVar[str] = "SecondExpTest"
    description: ClassVar[str] = "Test plugin for second experiment"
    experiment: ClassVar[Optional[str]] = "exp2"

    param2: str = "test"


class TestMetadataPluginRegistry:
    """Test the MetadataPluginRegistry class."""

    def test_creation(self):
        """Test registry creation."""
        registry = MetadataPluginRegistry()
        assert len(registry.list_plugins()) == 0
        assert len(registry.list_experiments()) == 0

    def test_register_plugin(self):
        """Test plugin registration."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestMetadataPlugin)

        plugins = registry.list_plugins()
        assert "TestPlugin" in plugins
        assert len(plugins) == 1

    def test_register_plugin_with_experiment(self):
        """Test experiment-specific plugin registration."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestExperimentPlugin)

        plugins = registry.list_plugins()
        assert "ExperimentTest" in plugins

        experiments = registry.list_experiments()
        assert "test_exp" in experiments

        exp_plugins = registry.list_plugins(experiment="test_exp")
        assert "ExperimentTest" in exp_plugins

    def test_register_duplicate_plugin(self):
        """Test that duplicate registration raises error."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestMetadataPlugin)

        # Should raise error without override
        with pytest.raises(ValueError, match="already registered"):
            registry.register_plugin(TestMetadataPlugin)

    def test_register_duplicate_plugin_with_override(self):
        """Test that duplicate registration works with override."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestMetadataPlugin)
        registry.register_plugin(TestMetadataPlugin, override=True)

        # Should not raise error with override=True
        plugins = registry.list_plugins()
        assert "TestPlugin" in plugins

    def test_get_plugin(self):
        """Test getting registered plugin."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestMetadataPlugin)

        plugin_class = registry.get_plugin("TestPlugin")
        assert plugin_class is TestMetadataPlugin

    def test_get_nonexistent_plugin(self):
        """Test getting non-existent plugin."""
        registry = MetadataPluginRegistry()

        plugin = registry.get_plugin("NonExistent")
        assert plugin is None

    def test_get_plugin_with_experiment(self):
        """Test getting experiment-specific plugin."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestExperimentPlugin)

        plugin_class = registry.get_plugin("ExperimentTest", experiment="test_exp")
        assert plugin_class is TestExperimentPlugin

    def test_instantiate_plugin(self):
        """Test plugin instantiation."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestMetadataPlugin)

        descriptor = MetadataDescriptor(metadata_class="TestPlugin", test_param="custom")
        instance = registry.instantiate_plugin(descriptor)

        assert isinstance(instance, TestMetadataPlugin)
        assert instance.test_param == "custom"

    def test_instantiate_plugin_with_experiment(self):
        """Test experiment plugin instantiation."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestExperimentPlugin)

        descriptor = MetadataDescriptor(metadata_class="ExperimentTest", experiment="test_exp", exp_param=99)
        instance = registry.instantiate_plugin(descriptor)

        assert isinstance(instance, TestExperimentPlugin)
        assert instance.exp_param == 99

    def test_instantiate_nonexistent_plugin(self):
        """Test instantiation of non-existent plugin."""
        registry = MetadataPluginRegistry()

        descriptor = MetadataDescriptor(metadata_class="NonExistent")

        with pytest.raises(KeyError, match="Unknown metadata plugin"):
            registry.instantiate_plugin(descriptor)

    def test_discover_plugins(self):
        """Test automatic plugin discovery."""
        registry = MetadataPluginRegistry()

        # Test discovery from a package that doesn't exist - should return 0
        discovered = registry.discover_plugins(["nonexistent.package"])
        assert discovered == 0

        # Test that the registry still works normally
        plugins = registry.list_plugins()
        assert isinstance(plugins, list)

    def test_list_experiments(self):
        """Test listing experiments."""
        registry = MetadataPluginRegistry()

        assert len(registry.list_experiments()) == 0

        registry.register_plugin(TestExperimentPlugin)  # exp1 = test_exp
        registry.register_plugin(TestSecondExperimentPlugin)  # exp2

        experiments = registry.list_experiments()
        assert "test_exp" in experiments
        assert "exp2" in experiments
        assert len(experiments) == 2

    def test_list_plugins_by_experiment(self):
        """Test listing plugins by experiment."""
        registry = MetadataPluginRegistry()

        registry.register_plugin(TestMetadataPlugin)  # No experiment
        registry.register_plugin(TestExperimentPlugin)  # test_exp experiment

        # All plugins
        all_plugins = registry.list_plugins()
        assert "TestPlugin" in all_plugins
        assert "ExperimentTest" in all_plugins

        # Experiment-specific plugins
        exp_plugins = registry.list_plugins(experiment="test_exp")
        assert "ExperimentTest" in exp_plugins
        assert "TestPlugin" not in exp_plugins


class TestGlobalRegistryFunctions:
    """Test the global registry functions."""

    def test_get_registry(self):
        """Test getting the global registry."""
        registry = get_registry()
        assert isinstance(registry, MetadataPluginRegistry)

        # Should return the same instance
        registry2 = get_registry()
        assert registry is registry2

    def test_list_registered(self):
        """Test listing registered plugins globally."""
        # This test depends on the global registry state
        plugins = list_registered()
        assert isinstance(plugins, list)
        # Should include core plugins
        assert "User" in plugins
        assert "Admin" in plugins

    def test_instantiate_metadata(self):
        """Test global metadata instantiation."""
        # Test with core plugin
        instance = instantiate_metadata("User", {})
        assert instance.metadata_type == "User"

    def test_instantiate_metadata_with_experiment(self):
        """Test global metadata instantiation with experiment."""
        # This would need to be tested with actual experiment plugins
        # For now, just test the function signature
        try:
            instantiate_metadata("User", {}, experiment="test")
        except ValueError:
            # Expected if experiment plugin doesn't exist
            pass

    def test_register_metadata(self):
        """Test global metadata registration."""
        # Test registering a new plugin

        class GlobalTestPlugin(BaseMetadataModel):
            metadata_type: ClassVar[str] = "GlobalTest"
            description: ClassVar[str] = "Global test plugin"

        register_metadata("GlobalTest", GlobalTestPlugin)

        # Should be able to instantiate it
        instance = instantiate_metadata("GlobalTest", {})
        assert isinstance(instance, GlobalTestPlugin)


class TestPluginSystem:
    """Test the modern plugin system functionality."""

    def test_direct_plugin_usage(self):
        """Test using plugins directly without legacy wrapper."""
        from dirac_cwl_proto.metadata.core import BaseMetadataModel

        class DirectPlugin(BaseMetadataModel):
            metadata_type: ClassVar[str] = "DirectTest"
            test_param: str = "default"

            def get_input_query(self, input_name: str, **kwargs: Any) -> Optional[Path]:
                return Path(f"/direct/{input_name}/{self.test_param}")

        # Register plugin directly
        register_metadata("DirectTest", DirectPlugin)

        # Should be able to instantiate
        instance = instantiate_metadata("DirectTest", {"test_param": "custom"})

        # Should work with new interface
        result = instance.get_input_query("test_input")
        assert result == Path("/direct/test_input/custom")

    def test_plugin_parameter_handling(self):
        """Test that parameters are passed correctly to plugins."""
        from dirac_cwl_proto.metadata.core import BaseMetadataModel

        class ParameterTestPlugin(BaseMetadataModel):
            test_param: str = "default"
            another_param: str = "default"

            metadata_type: ClassVar[str] = "ParameterTest"

        register_metadata("ParameterTest", ParameterTestPlugin)

        # Test with snake_case parameters (should work)
        instance = instantiate_metadata("ParameterTest", {"test_param": "value1", "another_param": "value2"})

        assert instance.test_param == "value1"
        assert instance.another_param == "value2"

"""Enhanced plugin registry for metadata models.

This module provides a sophisticated plugin discovery and registration system
for DIRAC metadata models, supporting experiment-specific extensions and
automatic discovery.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from typing import Any, Dict, Iterable, List, Optional, Type

from .core import BaseMetadataModel, MetadataDescriptor

logger = logging.getLogger(__name__)


class MetadataPluginRegistry:
    """Registry for metadata plugin discovery and management."""

    def __init__(self) -> None:
        self._plugins: Dict[str, Type[BaseMetadataModel]] = {}
        self._experiment_plugins: Dict[str, Dict[str, Type[BaseMetadataModel]]] = {}
        self._plugin_info: Dict[str, Dict[str, Any]] = {}

    def register_plugin(self, plugin_class: Type[BaseMetadataModel], override: bool = False) -> None:
        """Register a metadata plugin.

        Parameters
        ----------
        plugin_class : Type[BaseMetadataModel]
            The metadata model class to register.
        override : bool, optional
            Whether to override existing registrations, by default False.

        Raises
        ------
        ValueError
            If plugin is already registered and override=False.
        """
        if not issubclass(plugin_class, BaseMetadataModel):
            raise ValueError(f"Plugin {plugin_class} must inherit from BaseMetadataModel")

        plugin_key = plugin_class.metadata_type
        experiment = plugin_class.experiment

        # Check for conflicts
        if plugin_key in self._plugins and not override:
            existing = self._plugins[plugin_key]
            raise ValueError(
                f"Plugin '{plugin_key}' already registered by {existing.__module__}.{existing.__name__}. "
                f"Use override=True to replace."
            )

        # Register globally
        self._plugins[plugin_key] = plugin_class
        self._plugin_info[plugin_key] = plugin_class.get_schema_info()

        # Register by experiment if specified
        if experiment:
            if experiment not in self._experiment_plugins:
                self._experiment_plugins[experiment] = {}
            self._experiment_plugins[experiment][plugin_key] = plugin_class

        logger.info(
            f"Registered metadata plugin '{plugin_key}' "
            f"from {plugin_class.__module__}.{plugin_class.__name__}"
            f"{f' (experiment: {experiment})' if experiment else ''}"
        )

    def get_plugin(self, plugin_key: str, experiment: Optional[str] = None) -> Optional[Type[BaseMetadataModel]]:
        """Get a registered plugin.

        Parameters
        ----------
        plugin_key : str
            The plugin identifier.
        experiment : Optional[str], optional
            Experiment namespace to search first, by default None.

        Returns
        -------
        Optional[Type[BaseMetadataModel]]
            The plugin class or None if not found.
        """
        # Try experiment-specific first if specified
        if experiment and experiment in self._experiment_plugins:
            if plugin_key in self._experiment_plugins[experiment]:
                return self._experiment_plugins[experiment][plugin_key]

        # Fall back to global registry
        return self._plugins.get(plugin_key)

    def instantiate_plugin(self, descriptor: MetadataDescriptor, **kwargs: Any) -> BaseMetadataModel:
        """Instantiate a metadata plugin from a descriptor.

        Parameters
        ----------
        descriptor : MetadataDescriptor
            The metadata descriptor containing configuration.
        **kwargs : Any
            Additional parameters to pass to the plugin constructor.

        Returns
        -------
        BaseMetadataModel
            Instantiated metadata model.

        Raises
        ------
        KeyError
            If the requested plugin is not registered.
        ValueError
            If plugin instantiation fails.
        """
        plugin_class = self.get_plugin(descriptor.metadata_class, descriptor.experiment)

        if plugin_class is None:
            available = self.list_plugins(descriptor.experiment)
            raise KeyError(
                f"Unknown metadata plugin: '{descriptor.metadata_class}'"
                f"{f' for experiment {descriptor.experiment}' if descriptor.experiment else ''}. "
                f"Available: {available}"
            )

        # Extract plugin parameters from descriptor
        plugin_params = descriptor.model_dump(exclude={"metadata_class", "experiment", "version"})
        plugin_params.update(kwargs)

        try:
            return plugin_class(**plugin_params)
        except Exception as e:
            raise ValueError(f"Failed to instantiate plugin '{descriptor.metadata_class}': {e}") from e

    def list_plugins(self, experiment: Optional[str] = None) -> List[str]:
        """List available plugins.

        Parameters
        ----------
        experiment : Optional[str], optional
            Filter by experiment, by default None.

        Returns
        -------
        List[str]
            List of available plugin keys.
        """
        if experiment and experiment in self._experiment_plugins:
            return list(self._experiment_plugins[experiment].keys())
        return list(self._plugins.keys())

    def list_experiments(self) -> List[str]:
        """List available experiments."""
        return list(self._experiment_plugins.keys())

    def get_plugin_info(self, plugin_key: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a plugin."""
        return self._plugin_info.get(plugin_key)

    def discover_plugins(self, package_names: Optional[List[str]] = None) -> int:
        """Discover and register plugins from specified packages.

        Parameters
        ----------
        package_names : Optional[List[str]], optional
            Packages to search for plugins. If None, searches default locations.

        Returns
        -------
        int
            Number of plugins discovered and registered.
        """
        if package_names is None:
            package_names = [
                "dirac_cwl_proto.metadata.plugins",
                "diracx_metadata_plugins",  # Future extension package
            ]

        discovered = 0
        for package_name in package_names:
            discovered += self._discover_from_package(package_name)

        return discovered

    def _discover_from_package(self, package_name: str) -> int:
        """Discover plugins from a specific package."""
        try:
            package = importlib.import_module(package_name)
        except ImportError:
            logger.debug(f"Package {package_name} not found, skipping plugin discovery")
            return 0

        discovered = 0
        package_path = getattr(package, "__path__", None)
        if package_path is None:
            return 0

        for _importer, modname, ispkg in pkgutil.iter_modules(package_path):
            if ispkg:
                continue

            try:
                module_name = f"{package_name}.{modname}"
                module = importlib.import_module(module_name)

                # Look for metadata model classes
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, BaseMetadataModel) and attr is not BaseMetadataModel:
                        self.register_plugin(attr)
                        discovered += 1

            except Exception as e:
                logger.warning(f"Failed to import plugin module {module_name}: {e}")

        return discovered

    def validate_descriptor(self, descriptor: MetadataDescriptor) -> List[str]:
        """Validate a metadata descriptor against registered plugins.

        Parameters
        ----------
        descriptor : MetadataDescriptor
            The descriptor to validate.

        Returns
        -------
        List[str]
            List of validation errors (empty if valid).
        """
        errors = []

        plugin_class = self.get_plugin(descriptor.metadata_class, descriptor.experiment)

        if plugin_class is None:
            available = self.list_plugins(descriptor.experiment)
            errors.append(f"Unknown metadata plugin: '{descriptor.metadata_class}'. " f"Available: {available}")
            return errors

        # Validate descriptor against plugin schema
        try:
            plugin_params = descriptor.model_dump(exclude={"metadata_class", "experiment", "version"})
            plugin_class.model_validate(plugin_params)
        except Exception as e:
            errors.append(f"Plugin validation failed: {e}")

        return errors


# Global registry instance
_registry = MetadataPluginRegistry()


# Public API functions for backward compatibility
def register_metadata(name: str, cls: Type[BaseMetadataModel]) -> None:
    """Register a metadata class (backward compatibility)."""
    _registry.register_plugin(cls)


def get_metadata_class(name: str) -> Optional[Type[BaseMetadataModel]]:
    """Get metadata class by name (backward compatibility)."""
    return _registry.get_plugin(name)


def instantiate_metadata(name: str, params: Dict[str, Any], experiment: str | None = None) -> BaseMetadataModel:
    """Instantiate metadata from name and params (backward compatibility)."""
    descriptor = MetadataDescriptor(metadata_class=name, experiment=experiment, **params)
    return _registry.instantiate_plugin(descriptor)


def list_registered() -> Iterable[str]:
    """List registered metadata types (backward compatibility)."""
    return _registry.list_plugins()


# New public API
def get_registry() -> MetadataPluginRegistry:
    """Get the global metadata plugin registry."""
    return _registry


def discover_plugins(package_names: Optional[List[str]] = None) -> int:
    """Discover and register plugins from packages."""
    return _registry.discover_plugins(package_names)

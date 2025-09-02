"""Enhanced plugin registry for metadata models.

This module provides a sophisticated plugin discovery and registration system
for DIRAC metadata models, supporting virtual organization-specific extensions and
automatic discovery.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from typing import Any, Dict, List, Optional, Type

from .core import DataManager, TaskRuntimeBasePlugin

logger = logging.getLogger(__name__)


class MetadataPluginRegistry:
    """Registry for metadata plugin discovery and management."""

    def __init__(self) -> None:
        self._plugins: Dict[str, Type[TaskRuntimeBasePlugin]] = {}
        self._vo_plugins: Dict[str, Dict[str, Type[TaskRuntimeBasePlugin]]] = {}
        self._plugin_info: Dict[str, Dict[str, Any]] = {}

    def register_plugin(
        self, plugin_class: Type[TaskRuntimeBasePlugin], override: bool = False
    ) -> None:
        """Register a metadata plugin.

        Parameters
        ----------
        plugin_class : Type[TaskRuntimeBasePlugin]
            The metadata model class to register.
        override : bool, optional
            Whether to override existing registrations, by default False.

        Raises
        ------
        ValueError
            If plugin is already registered and override=False.
        """
        if not issubclass(plugin_class, TaskRuntimeBasePlugin):
            raise ValueError(
                f"Plugin {plugin_class} must inherit from TaskRuntimeBasePlugin"
            )

        plugin_key = plugin_class.get_metadata_class()
        vo = plugin_class.vo

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

        # Register by VO if specified
        if vo:
            if vo not in self._vo_plugins:
                self._vo_plugins[vo] = {}
            self._vo_plugins[vo][plugin_key] = plugin_class

        logger.info(
            f"Registered metadata plugin '{plugin_key}' "
            f"from {plugin_class.__module__}.{plugin_class.__name__}"
            f"{f' (VO: {vo})' if vo else ''}"
        )

    def get_plugin(
        self, plugin_key: str, vo: Optional[str] = None
    ) -> Optional[Type[TaskRuntimeBasePlugin]]:
        """Get a registered plugin.

        Parameters
        ----------
        plugin_key : str
            The plugin identifier.
        vo : Optional[str], optional
            Virtual Organization namespace to search first, by default None.

        Returns
        -------
        Optional[Type[TaskRuntimeBasePlugin]]
            The plugin class or None if not found.
        """
        # Try VO-specific first if specified
        if vo and vo in self._vo_plugins:
            if plugin_key in self._vo_plugins[vo]:
                return self._vo_plugins[vo][plugin_key]

        # Fall back to global registry
        return self._plugins.get(plugin_key)

    def instantiate_plugin(
        self, descriptor: DataManager, **kwargs: Any
    ) -> TaskRuntimeBasePlugin:
        """Instantiate a metadata plugin from a descriptor.

        Parameters
        ----------
        descriptor : DataManager
            The data manager containing configuration.
        **kwargs : Any
            Additional parameters to pass to the plugin constructor.

        Returns
        -------
        TaskRuntimeBasePlugin
            Instantiated metadata model.

        Raises
        ------
        KeyError
            If the requested plugin is not registered.
        ValueError
            If plugin instantiation fails.
        """
        plugin_class = self.get_plugin(descriptor.metadata_class, descriptor.vo)

        if plugin_class is None:
            available = self.list_plugins(descriptor.vo)
            raise KeyError(
                f"Unknown metadata plugin: '{descriptor.metadata_class}'"
                f"{f' for VO {descriptor.vo}' if descriptor.vo else ''}. "
                f"Available: {available}"
            )

        # Extract plugin parameters from descriptor
        plugin_params = descriptor.model_dump(
            exclude={"metadata_class", "vo", "version"}
        )
        plugin_params.update(kwargs)

        try:
            return plugin_class(**plugin_params)
        except Exception as e:
            raise ValueError(
                f"Failed to instantiate plugin '{descriptor.metadata_class}': {e}"
            ) from e

    def list_plugins(self, vo: Optional[str] = None) -> List[str]:
        """List available plugins.

        Parameters
        ----------
        vo : Optional[str], optional
            Filter by Virtual Organization, by default None.

        Returns
        -------
        List[str]
            List of available plugin keys.
        """
        if vo and vo in self._vo_plugins:
            return list(self._vo_plugins[vo].keys())
        return list(self._plugins.keys())

    def list_virtual_organizations(self) -> List[str]:
        """List available Virtual Organizations."""
        return list(self._vo_plugins.keys())

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
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, TaskRuntimeBasePlugin)
                        and attr is not TaskRuntimeBasePlugin
                    ):
                        self.register_plugin(attr)
                        discovered += 1

            except Exception as e:
                logger.warning(f"Failed to import plugin module {module_name}: {e}")

        return discovered

    def validate_descriptor(self, descriptor: DataManager) -> List[str]:
        """Validate a data manager against registered plugins.

        Parameters
        ----------
        descriptor : DataManager
            The data manager to validate.

        Returns
        -------
        List[str]
            List of validation errors (empty if valid).
        """
        errors = []

        plugin_class = self.get_plugin(descriptor.metadata_class, descriptor.vo)

        if plugin_class is None:
            available = self.list_plugins(descriptor.vo)
            errors.append(
                f"Unknown metadata plugin: '{descriptor.metadata_class}'. "
                f"Available: {available}"
            )
            return errors

        # Validate descriptor against plugin schema
        try:
            plugin_params = descriptor.model_dump(
                exclude={"metadata_class", "vo", "version"}
            )
            plugin_class.model_validate(plugin_params)
        except Exception as e:
            errors.append(f"Plugin validation failed: {e}")

        return errors


# Global registry instance
_registry = MetadataPluginRegistry()


# Public API
def get_registry() -> MetadataPluginRegistry:
    """Get the global metadata plugin registry."""
    return _registry


def discover_plugins(package_names: Optional[List[str]] = None) -> int:
    """Discover and register plugins from packages."""
    return _registry.discover_plugins(package_names)

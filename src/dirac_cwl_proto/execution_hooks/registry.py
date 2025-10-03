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

from .core import ExecutionHooksBasePlugin, ExecutionHooksHint

logger = logging.getLogger(__name__)


class ExecutionHooksPluginRegistry:
    """
    Registry for execution hooks plugins.

    This class manages the registration and retrieval of execution hooks plugins
    for different steps in CWL workflows. Plugins are registered using
    entry points and can be retrieved by name.
    """

    def __init__(self) -> None:
        self._plugins: Dict[str, Type[ExecutionHooksBasePlugin]] = {}
        self._vo_plugins: Dict[str, Dict[str, Type[ExecutionHooksBasePlugin]]] = {}
        self._plugin_info: Dict[str, Dict[str, Any]] = {}

    def register_plugin(
        self, plugin_class: Type[ExecutionHooksBasePlugin], override: bool = False
    ) -> None:
        """Register a metadata plugin.

        Parameters
        ----------
        plugin_class : Type[ExecutionHooksBasePlugin]
            The metadata model class to register.
        override : bool, optional
            Whether to override existing registrations, by default False.

        Raises
        ------
        ValueError
            If plugin is already registered and override=False.
        """
        if not issubclass(plugin_class, ExecutionHooksBasePlugin):
            raise ValueError(
                f"Plugin {plugin_class} must inherit from ExecutionHooksBasePlugin"
            )

        plugin_key = plugin_class.name()
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
    ) -> Optional[Type[ExecutionHooksBasePlugin]]:
        """Get a registered plugin.

        Parameters
        ----------
        plugin_key : str
            The plugin identifier.
        vo : Optional[str], optional
            Virtual Organization namespace to search first, by default None.

        Returns
        -------
        Optional[Type[ExecutionHooksBasePlugin]]
            The plugin class or None if not found.
        """
        # Try VO-specific first if specified
        if vo and vo in self._vo_plugins:
            if plugin_key in self._vo_plugins[vo]:
                return self._vo_plugins[vo][plugin_key]

        # Fall back to global registry
        return self._plugins.get(plugin_key)

    def instantiate_plugin(
        self, descriptor: ExecutionHooksHint, **kwargs: Any
    ) -> ExecutionHooksBasePlugin:
        """Instantiate a metadata plugin from a descriptor.

        Parameters
        ----------
        descriptor : ExecutionHooksHint
            The data manager containing configuration.
        **kwargs : Any
            Additional parameters to pass to the plugin constructor.

        Returns
        -------
        ExecutionHooksBasePlugin
            Instantiated metadata model.

        Raises
        ------
        KeyError
            If the requested plugin is not registered.
        ValueError
            If plugin instantiation fails.
        """
        plugin_class = self.get_plugin(descriptor.hook_plugin)

        if plugin_class is None:
            available = self.list_plugins()
            raise KeyError(
                f"Unknown execution hooks plugin: '{descriptor.hook_plugin}'"
                f"Available: {available}"
            )

        # Extract plugin parameters from descriptor
        plugin_params = descriptor.model_dump(
            exclude={
                "hook_plugin",
            }
        )
        plugin_params.update(kwargs)

        try:
            return plugin_class(**plugin_params)
        except Exception as e:
            raise ValueError(
                f"Failed to instantiate plugin '{descriptor.hook_plugin}': {e}"
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
                "dirac_cwl_proto.execution_hooks.plugins",
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
                        and issubclass(attr, ExecutionHooksBasePlugin)
                        and attr is not ExecutionHooksBasePlugin
                    ):
                        self.register_plugin(attr)
                        discovered += 1

            except Exception as e:
                logger.warning(f"Failed to import plugin module {module_name}: {e}")

        return discovered

    def validate_descriptor(self, descriptor: ExecutionHooksHint) -> List[str]:
        """Validate a data manager against registered plugins.

        Parameters
        ----------
        descriptor : ExecutionHooksHint
            The data manager to validate.

        Returns
        -------
        List[str]
            List of validation errors (empty if valid).
        """
        errors = []

        plugin_class = self.get_plugin(descriptor.hook_plugin)

        if plugin_class is None:
            available = self.list_plugins()
            errors.append(
                f"Unknown metadata plugin: '{descriptor.hook_plugin}'. "
                f"Available: {available}"
            )
            return errors

        # Validate descriptor against plugin schema
        try:
            plugin_params = descriptor.model_dump(exclude={"hook_plugin"})
            plugin_class.model_validate(plugin_params)
        except Exception as e:
            errors.append(f"Plugin validation failed: {e}")

        return errors


# Global registry instance
_registry = ExecutionHooksPluginRegistry()


# Public API
def get_registry() -> ExecutionHooksPluginRegistry:
    """Get the global execution hooks plugin registry."""
    return _registry


def discover_plugins(package_names: Optional[List[str]] = None) -> int:
    """Discover and register plugins from packages."""
    return _registry.discover_plugins(package_names)

"""Enhanced metadata registry for DIRAC CWL integration.

This module provides a comprehensive plugin system for metadata models,
supporting virtual organization-specific extensions and automatic discovery.

The module maintains backward compatibility with the original API while
providing enhanced functionality through the new plugin system.
"""

from .core import (
    BaseMetadataModel,
    DataCatalogInterface,
    MetadataDescriptor,
    MetadataProcessor,
    TaskDescriptor,
    TransformationMetadataDescriptor,
)
from .registry import (
    MetadataPluginRegistry,
    discover_plugins,
    get_metadata_class,
    get_registry,
    instantiate_metadata,
    list_registered,
    register_metadata,
)

# Initialize the registry and discover core plugins
_registry = get_registry()

# Auto-discover plugins on import
try:
    discover_plugins()
except Exception:
    # Fail silently if plugin discovery fails during import
    pass

__all__ = [
    # Core classes
    "BaseMetadataModel",
    "DataCatalogInterface",
    "MetadataDescriptor",
    "MetadataProcessor",
    "TaskDescriptor",
    "TransformationMetadataDescriptor",
    # Registry functions
    "MetadataPluginRegistry",
    "discover_plugins",
    "get_metadata_class",
    "get_registry",
    "instantiate_metadata",
    "list_registered",
    "register_metadata",
]

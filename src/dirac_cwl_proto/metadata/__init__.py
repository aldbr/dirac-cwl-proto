"""Simple metadata registry for DIRAC CWL integration.

This module provides a tiny registry so downstream projects can register
their own metadata classes (Pydantic models or plain callables) by name and
instantiate them from descriptor dictionaries parsed from CWL hints.

Usage:
    from dirac_cwl_proto.metadata import register_metadata, instantiate_metadata

    register_metadata("MyType", MyMetadataClass)
    instance = instantiate_metadata("MyType", {"foo": "bar"})
"""
import logging
from typing import Any, Callable, Iterable

logger = logging.getLogger(__name__)

# Registry mapping short name -> callable/class. Callable must accept **kwargs to
# build an instance (Pydantic BaseModel classes satisfy this).
_REGISTRY: dict[str, Callable[..., Any]] = {}


def register_metadata(name: str, cls: Callable[..., Any]) -> None:
    """Register a metadata class under a short name.

    If the name already exists it will be overwritten and a debug message will
    be logged.
    """
    if name in _REGISTRY:
        logger.debug("Overwriting metadata registration for %s", name)
    _REGISTRY[name] = cls


def get_metadata_class(name: str) -> Callable[..., Any] | None:
    """Return the registered class/callable for ``name`` or None."""
    return _REGISTRY.get(name)


def instantiate_metadata(name: str, params: dict[str, Any]) -> Any:
    """Instantiate the metadata class registered under ``name`` with ``params``.

    Raises KeyError when ``name`` is unknown and TypeError/ValueError when
    instantiation fails.
    """
    cls = get_metadata_class(name)
    if cls is None:
        raise KeyError(f"Unknown metadata type: {name}")
    # Most metadata classes will be Pydantic BaseModel subclasses and accept
    # kwargs. Call the class to build an instance.
    return cls(**params) if params is not None else cls()


def list_registered() -> Iterable[str]:
    """Yield registered metadata type names."""
    return iter(_REGISTRY.keys())

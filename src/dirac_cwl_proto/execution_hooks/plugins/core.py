"""Core DIRAC metadata models.

This module contains the standard metadata models provided by DIRAC core.
These serve as examples and provide basic functionality for common use cases.
"""

from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field

from ..core import (
    ExecutionHooksBasePlugin,
)


class QueryBasedPlugin(ExecutionHooksBasePlugin):
    """Metadata plugin using LFN-based data catalog for structured data discovery.

    This plugin demonstrates filesystem-based data organization using
    Logical File Names (LFNs) with campaign, site, and data type parameters.
    """

    description: ClassVar[str] = "LFN-based metadata for structured data discovery"

    # LFN parameters
    query_root: str = Field(
        default="/grid/data", description="Base path for LFN structure"
    )
    site: Optional[str] = Field(
        default=None, description="Site identifier for LFN path"
    )
    campaign: Optional[str] = Field(
        default=None, description="Campaign name for LFN path"
    )
    data_type: Optional[str] = Field(
        default=None, description="Data type classification"
    )

    def __init__(self, **data):
        query_root = data.get("query_root", "/grid/data")
        data.setdefault("base_path", query_root)
        super().__init__(**data)

"""Core DIRAC metadata models.

This module contains the standard metadata models provided by DIRAC core.
These serve as examples and provide basic functionality for common use cases.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from pydantic import Field

from ..core import (
    DataCatalogInterface,
    DummyDataCatalogInterface,
    ExecutionHooksBasePlugin,
)


class QueryBasedDataCatalogInterface(DataCatalogInterface):
    """Data catalog interface for query-based data discovery.

    This interface builds paths based on query parameters like campaign,
    site, and data type for structured data organization.
    """

    def __init__(
        self,
        query_root: str = "/",
        site: Optional[str] = None,
        campaign: Optional[str] = None,
        data_type: Optional[str] = None,
    ):
        self.query_root = query_root
        self.site = site
        self.campaign = campaign
        self.data_type = data_type

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Generate input query based on metadata parameters."""
        base_path = Path(self.query_root) if self.query_root else Path("filecatalog")

        # Build query path from metadata
        query_parts = []
        if self.campaign:
            query_parts.append(self.campaign)
        if self.site:
            query_parts.append(self.site)
        if self.data_type:
            query_parts.append(self.data_type)

        if query_parts:
            return base_path / Path(*query_parts)

        return None

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Generate output path based on metadata parameters."""
        base_path = Path("filecatalog") / "outputs"

        if self.campaign and self.site:
            return base_path / self.campaign / self.site
        elif self.campaign:
            return base_path / self.campaign
        else:
            return base_path


class UserPlugin(ExecutionHooksBasePlugin):
    """Default user plugin with no special processing.

    This is the simplest plugin that performs no special input/output
    processing and is suitable for basic job execution.
    """

    description: ClassVar[str] = "Basic user plugin with no special processing"

    def __init__(self, **data):
        super().__init__(**data)
        self.data_catalog = DummyDataCatalogInterface()


class AdminPlugin(ExecutionHooksBasePlugin):
    """Administrative plugin with enhanced logging.

    This plugin provides additional logging and monitoring
    capabilities for administrative tasks.

    Parameters
    ----------
    log_level : str, optional
        Logging level to use. Defaults to "INFO".
    enable_monitoring : bool, optional
        Whether to enable enhanced monitoring. Defaults to True.
    admin_level : int, optional
        Administrative privilege level. Defaults to 1.
    """

    description: ClassVar[str] = "Administrative plugin with enhanced logging"

    log_level: str = "INFO"
    enable_monitoring: bool = True
    admin_level: int = 1

    def __init__(self, **data):
        super().__init__(**data)
        self.data_catalog = DummyDataCatalogInterface()

    def pre_process(
        self, job_path: Path, command: List[str], **kwargs: Any
    ) -> List[str]:
        """Add logging configuration to command."""
        if self.log_level != "INFO":
            command.extend(["--log-level", self.log_level])
        return command

    def post_process(self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any) -> bool:
        """Enhanced post-processing with monitoring."""
        if self.enable_monitoring:
            # Could send metrics to monitoring system
            pass
        return True


class QueryBasedPlugin(ExecutionHooksBasePlugin):
    """Metadata plugin that supports query-based input resolution.

    This plugin demonstrates how to implement query-based data discovery
    using metadata parameters.
    """

    description: ClassVar[str] = "Query-based metadata for data discovery"

    # Query parameters
    query_root: str = Field(default="/", description="Root path for queries")
    site: Optional[str] = Field(default=None, description="Site to query")
    campaign: Optional[str] = Field(default=None, description="Campaign name")
    data_type: Optional[str] = Field(default=None, description="Data type")

    def __init__(self, **data):
        super().__init__(**data)
        # Create data catalog with current parameters
        self.data_catalog = QueryBasedDataCatalogInterface(
            query_root=self.query_root,
            site=self.site,
            campaign=self.campaign,
            data_type=self.data_type,
        )

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Generate input query based on metadata parameters.

        Parameters
        ----------
        input_name : str
            Name of the input parameter.
        **kwargs : Any
            Additional query parameters.

        Returns
        -------
        Union[Path, List[Path], None]
            Path(s) to input data based on query parameters.
        """
        base_path = Path(self.query_root) if self.query_root else Path("filecatalog")

        # Build query path from metadata
        query_parts = []
        if self.campaign:
            query_parts.append(self.campaign)
        if self.site:
            query_parts.append(self.site)
        if self.data_type:
            query_parts.append(self.data_type)

        if query_parts:
            return base_path / Path(*query_parts)

        return None

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Generate output path based on metadata parameters."""
        base_path = Path("filecatalog") / "outputs"

        if self.campaign and self.site:
            return base_path / self.campaign / self.site
        elif self.campaign:
            return base_path / self.campaign

        return base_path / "default"


class TaskQueryDataCatalogInterface(DataCatalogInterface):
    """Simple data catalog interface for task query example."""

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Generate query paths based on site and campaign metadata."""
        site = kwargs.get("site", "")
        campaign = kwargs.get("campaign", "")

        # Example implementation
        if site and campaign:
            return [Path("filecatalog") / campaign / site]
        elif site:
            return Path("filecatalog") / site
        else:
            return None

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Simple output path generation."""
        return Path("filecatalog") / "outputs"


class TaskWithMetadataQueryPlugin(ExecutionHooksBasePlugin):
    """Metadata plugin that demonstrates query-based input resolution.

    This class provides methods to query metadata and generate input paths
    based on metadata parameters like site and campaign.

    This is primarily used as an example of how to implement query-based
    data discovery in the DIRAC metadata system.
    """

    description: ClassVar[
        str
    ] = "Example metadata plugin with query-based input resolution"

    def __init__(self, **kwargs: Any):
        """Initialize with task query data catalog interface."""
        super().__init__(**kwargs)
        self.data_catalog = TaskQueryDataCatalogInterface()

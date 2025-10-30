"""Core DIRAC metadata models.

This module contains the standard metadata models provided by DIRAC core.
These serve as examples and provide basic functionality for common use cases.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    Workflow,
)
from pydantic import Field

from ..core import (
    ExecutionHooksBasePlugin,
)


class UserPlugin(ExecutionHooksBasePlugin):
    """Default user plugin with no special processing.

    This is the simplest plugin that performs no special input/output
    processing and is suitable for basic job execution.
    """

    description: ClassVar[str] = "Basic user plugin with no special processing"


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

    def pre_process(
        self,
        executable: CommandLineTool | Workflow | ExpressionTool,
        arguments: Any | None,
        job_path: Path,
        command: List[str],
        **kwargs: Any,
    ) -> List[str]:
        """Add logging configuration to command."""
        command = super().pre_process(
            executable, arguments, job_path, command, **kwargs
        )
        if self.log_level != "INFO":
            command.extend(["--log-level", self.log_level])
        return command

    def post_process(
        self, job_path: Path, stdout: Optional[str] = None, **kwargs: Any
    ) -> bool:
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

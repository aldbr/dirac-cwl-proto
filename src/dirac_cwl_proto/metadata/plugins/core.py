"""Core DIRAC metadata models.

This module contains the standard metadata models provided by DIRAC core.
These serve as examples and provide basic functionality for common use cases.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from ..core import BaseMetadataModel


class UserMetadata(BaseMetadataModel):
    """Default user metadata model with no special processing.

    This is the simplest metadata model that performs no special input/output
    processing and is suitable for basic job execution.
    """

    metadata_type: ClassVar[str] = "User"
    description: ClassVar[str] = "Basic user metadata with no special processing"


class AdminMetadata(BaseMetadataModel):
    """Administrative metadata model with enhanced logging.

    This metadata model provides additional logging and monitoring
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

    metadata_type: ClassVar[str] = "Admin"
    description: ClassVar[str] = "Administrative metadata with enhanced logging"

    log_level: str = "INFO"
    enable_monitoring: bool = True
    admin_level: int = 1

    def pre_process(self, job_path: Path, command: List[str]) -> List[str]:
        """Add logging configuration to command."""
        if self.log_level != "INFO":
            command.extend(["--log-level", self.log_level])
        return command

    def post_process(self, job_path: Path) -> bool:
        """Enhanced post-processing with monitoring."""
        if self.enable_monitoring:
            # Could send metrics to monitoring system
            pass
        return True


class QueryBasedMetadata(BaseMetadataModel):
    """Metadata model that supports query-based input resolution.

    This model demonstrates how to implement query-based data discovery
    using metadata parameters.
    """

    metadata_type: ClassVar[str] = "QueryBased"
    description: ClassVar[str] = "Metadata with query-based input resolution"

    # Query parameters
    query_root: Optional[str] = None
    site: Optional[str] = None
    campaign: Optional[str] = None
    data_type: Optional[str] = None

    def get_input_query(self, input_name: str, **kwargs: Any) -> Union[Path, List[Path], None]:
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

    def get_output_query(self, output_name: str) -> Optional[Path]:
        """Generate output path based on metadata parameters."""
        base_path = Path("filecatalog") / "outputs"

        if self.campaign and self.site:
            return base_path / self.campaign / self.site
        elif self.campaign:
            return base_path / self.campaign

        return base_path / "default"

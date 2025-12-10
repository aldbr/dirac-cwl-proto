"""Core DIRAC metadata models.

This module contains the standard metadata models provided by DIRAC core.
These serve as examples and provide basic functionality for common use cases.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Sequence, Union

from DIRACCommon.Core.Utilities.ReturnValues import (  # type: ignore[import-untyped]
    returnSingleResult,
    returnValueOrRaise,
)
from pydantic import Field

from ..core import (
    ExecutionHooksBasePlugin,
)

logger = logging.getLogger(__name__)


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

    def get_input_query(
        self, input_name: str, **kwargs: Any
    ) -> Union[Path, List[Path], None]:
        """Generate LFN-based input query path.

        Accepts and ignores extra kwargs for interface compatibility.
        """
        # Build LFN: /query_root/vo/campaign/site/data_type/input_name
        path_parts = []

        if self.vo:
            path_parts.append(self.vo)

        if self.campaign:
            path_parts.append(self.campaign)
        if self.site:
            path_parts.append(self.site)
        if self.data_type:
            path_parts.append(self.data_type)

        if len(path_parts) > 0:  # More than just VO
            return Path(self.query_root) / Path(*path_parts) / Path(input_name)

        return Path(self.query_root) / Path(input_name)

    def get_output_query(self, output_name: str, **kwargs: Any) -> Optional[Path]:
        """Generate LFN-based output path.

        Accepts and ignores extra kwargs for interface compatibility.
        """
        # Output path: /grid/data/vo/outputs/campaign/site
        output_base = Path(self.query_root)
        if self.vo:
            output_base = output_base / self.vo
        output_base = output_base / "outputs"

        if self.campaign:
            output_base = output_base / self.campaign
        if self.site:
            output_base = output_base / self.site

        return output_base

    def store_output(
        self,
        output_name: str,
        src_path: str | Path | Sequence[str | Path],
        **kwargs: Any,
    ) -> None:
        """Store an output file or set of files via the appropriate storage interface.

        :param str output_name:
            The logical name of the output to store, used to determine the storage
            target (sandbox or output path).
        :param str | Path | Sequence[str | Path] src_path:
            The path or list of paths to the source file(s) to be stored.
            Can be a single path (string or Path) or a sequence of paths.
        :param Any **kwargs:
            Additional keyword arguments for extensibility.
        """
        logger.info(f"Storing output {output_name}, with source {src_path}")

        if not src_path:
            raise RuntimeError(
                f"src_path parameter required for filesystem storage of {output_name}"
            )
        if self.output_sandbox and output_name in self.output_sandbox:
            if isinstance(src_path, Path) or isinstance(src_path, str):
                src_path = [src_path]
            sb_path = returnValueOrRaise(
                self._sandbox_store_client.uploadFilesAsSandbox(src_path)
            )
            logger.info(
                f"Successfully stored output {output_name} in Sandbox {sb_path}"
            )
        else:
            if self.output_paths and output_name in self.output_paths:
                lfn = self.output_paths[output_name]
            else:
                lfn = self.get_output_query(output_name)

            if lfn:
                if isinstance(src_path, str) or isinstance(src_path, Path):
                    src_path = [src_path]
                for src in src_path:
                    file_lfn = Path(lfn) / Path(src).name
                    res = None
                    for se in self.output_se:
                        res = returnSingleResult(
                            self._datamanager.putAndRegister(str(file_lfn), src, se)
                        )
                        if res["OK"]:
                            logger.info(
                                f"Successfully saved file {src} with LFN {file_lfn}"
                            )
                            break
                    if res and not res["OK"]:
                        raise RuntimeError(
                            f"Could not save file {src} with LFN {str(file_lfn)} : {res['Message']}"
                        )

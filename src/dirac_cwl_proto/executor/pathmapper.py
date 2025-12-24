"""Custom PathMapper for handling DIRAC LFNs in CWL workflows."""

import os
import logging
from typing import Any, Dict, List, Optional, Tuple, cast
from cwltool.pathmapper import PathMapper, MapperEnt
from cwltool.utils import CWLObjectType
from dirac_cwl_proto.job.replica_catalog import ReplicaCatalog

logger = logging.getLogger("dirac-cwl-run")


class DiracPathMapper(PathMapper):
    """PathMapper that can resolve LFN: URIs using a replica catalog.

    This extends PathMapper to intercept files with LFN: URIs and resolve them
    to physical file paths using the replica catalog before processing.
    """

    def __init__(
        self,
        referenced_files: List[CWLObjectType],
        basedir: str,
        stagedir: str,
        separateDirs: bool = True,
        replica_catalog: Optional[ReplicaCatalog] = None,
    ):
        """Initialize with optional replica catalog.

        Args:
            referenced_files: Files referenced in the CWL workflow
            basedir: Base directory for relative paths
            stagedir: Staging directory for files
            separateDirs: Whether to separate directories
            replica_catalog: ReplicaCatalog for LFN resolution
        """
        self.replica_catalog = replica_catalog or ReplicaCatalog(root={})
        super().__init__(referenced_files, basedir, stagedir, separateDirs)

    def visit(
        self,
        obj: CWLObjectType,
        stagedir: str,
        basedir: str,
        copy: bool = False,
        staged: bool = False,
    ) -> None:
        """Visit a file object, handling LFN: URIs.

        LFN: URIs are resolved to their physical file locations (PFNs) using the
        replica catalog. The PFN can be:
        - A local file path (file://...)
        - A remote URL (root://... for xrootd, etc.)

        CWL will use these paths/URLs directly without staging (copying/linking).
        The files are either already downloaded locally or will be accessed via
        network protocols like xrootd.
        """
        tgt = obj.get("location", "")
        logger.debug(f"DiracPathMapper.visit: processing location={tgt}")

        # Check if this is an LFN that we need to resolve
        if tgt.startswith("LFN:") and obj["class"] == "File":
            # Extract the LFN (without the LFN: prefix)
            lfn = tgt[4:]  # Remove "LFN:" prefix
            logger.debug(f"DiracPathMapper.visit: Found LFN={lfn}")

            # Look up in replica catalog to resolve to PFN
            if lfn in self.replica_catalog.root:
                entry = self.replica_catalog[lfn]
                if entry.replicas:
                    # Get the first replica's URL (can be file:// or root:// or https:// etc.)
                    pfn = str(entry.replicas[0].url)
                    logger.info(f"DiracPathMapper: Resolved LFN:{lfn} -> {pfn}")

                    # For LFN-resolved files, we don't download or stage them
                    # We just map the original LFN location to the PFN
                    # The PFN will be used directly by the tools (via xrootd, https, etc.)
                    # Set both resolved and target to the PFN so CWL uses it directly
                    self._pathmap[tgt] = MapperEnt(
                        resolved=pfn,  # The physical URL/path
                        target=pfn,    # Use the PFN directly (not a staging path)
                        type="File",
                        staged=False,  # We're not staging/copying this file
                    )

                    # Add size from catalog if available
                    if entry.size_bytes is not None and "size" not in obj:
                        obj["size"] = entry.size_bytes

                    # Store checksum if available
                    if entry.checksum and "checksum" not in obj:
                        if entry.checksum.adler32:
                            # Format: "adler32$788c5caa"
                            obj["checksum"] = f"adler32${entry.checksum.adler32}"

                    # Handle secondary files if any
                    self.visitlisting(
                        cast(List[CWLObjectType], obj.get("secondaryFiles", [])),
                        stagedir,
                        basedir,
                        copy=copy,
                        staged=staged,
                    )

                    # Don't call parent visit - we've handled this completely
                    return

                else:
                    logger.warning(f"DiracPathMapper: LFN {lfn} in catalog but has no replicas")
            else:
                # LFN not in catalog - this will likely fail later
                logger.error(f"DiracPathMapper: LFN {lfn} NOT in catalog! Available LFNs: {list(self.replica_catalog.root.keys())[:5]}")

        # For non-LFN files or when LFN resolution failed, delegate to parent class
        super().visit(obj, stagedir, basedir, copy, staged)

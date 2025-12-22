# src/dirac_cwl_proto/cwltool_extensions/executor.py

from pathlib import Path
from typing import Any, Dict, Tuple
import logging
from cwltool.context import RuntimeContext
from cwltool.process import Process
from cwltool.executors import SingleJobExecutor
from cwltool.workflow import Workflow
from dirac_cwl_proto.job.replica_catalog import ReplicaCatalog

logger = logging.getLogger(__name__)

class DiracExecutor(SingleJobExecutor):
    """Custom executor that handles replica catalog management between steps."""
    
    def __init__(self, master_catalog_path: Path | None = None):
        super().__init__()
        self.master_catalog_path = master_catalog_path
        self.master_catalog: ReplicaCatalog | None = None
        
    def __call__(
        self,
        process: Process,
        job_order_object: Dict[str, Any],
        runtime_context: RuntimeContext,
        logger=logger
    ) -> Tuple[Dict[str, Any] | None, str]:
        """Execute process with replica catalog management.

        This method handles both CommandLineTools and Workflows (subworkflows).
        For Workflows, we ensure the executor is propagated to nested steps.
        """

        # Check if this is a Workflow (subworkflow) vs CommandLineTool
        is_workflow = isinstance(process, Workflow)

        if is_workflow:
            # For subworkflows, ensure our executor is used for nested steps
            # We don't create step catalogs at the workflow level, only for actual CommandLineTools
            logger.debug(f"Executing subworkflow: {process.tool.get('id', 'unknown')}")

            # Make sure the runtime context uses this executor for nested steps
            # This ensures catalog management happens for each CommandLineTool inside
            if runtime_context.default_container is None:
                runtime_context = runtime_context.copy()

            # The executor will be called recursively for each step in the subworkflow
            result = super().__call__(process, job_order_object, runtime_context, logger)
        else:
            # For CommandLineTools, do catalog management
            logger.debug(f"Executing CommandLineTool: {process.tool.get('id', 'unknown')}")

            # Before execution: prepare replica catalog for this step
            self._prepare_step_catalog(process, job_order_object, runtime_context)

            # Execute the process
            result = super().__call__(process, job_order_object, runtime_context, logger)

            # After execution: update catalog with outputs
            self._update_catalog_with_outputs(process, result, runtime_context)

        return result
    
    def _prepare_step_catalog(
        self,
        process: Process,
        job_order: Dict[str, Any],
        runtime_context: RuntimeContext
    ):
        """Filter master catalog for current step inputs and inject into runtime."""
        
        # Load master catalog if not already loaded
        if self.master_catalog is None:
            if self.master_catalog_path and self.master_catalog_path.exists():
                logger.info(f"Loading master replica catalog from {self.master_catalog_path}")
                self.master_catalog = ReplicaCatalog.model_validate_json(
                    self.master_catalog_path.read_text()
                )
            else:
                logger.info("No master catalog available, will create empty step catalog")
                # Don't return - we still need to create an empty catalog

        # Extract LFNs from current step inputs
        step_lfns = self._extract_lfns_from_inputs(job_order)

        # Generate the step catalog
        # For steps with input LFNs, filter from master catalog
        # For steps without inputs (e.g., simulation), create empty catalog
        if step_lfns and self.master_catalog:
            step_catalog = ReplicaCatalog(
                root={
                    lfn: entry
                    for lfn, entry in self.master_catalog.root.items()
                    if lfn in step_lfns
                }
            )
        else:
            # Create empty catalog for steps with no LFN inputs
            # This allows output files to be added by the step
            step_catalog = ReplicaCatalog(root={})

        # Write step catalog to a location accessible by the job
        step_catalog_path = Path(runtime_context.outdir) / "replica_catalog.json"
        step_catalog_path.write_text(step_catalog.model_dump_json(indent=2))

        logger.info(f"Created step replica catalog with {len(step_catalog.root)} entries")
    
    def _extract_lfns_from_inputs(self, job_order: Dict[str, Any]) -> list[str]:
        """Extract LFN paths from job inputs."""
        lfns = []
        
        def extract_recursive(obj):
            if isinstance(obj, dict):
                # Check if this looks like a File with an LFN path
                if "class" in obj and obj["class"] == "File":
                    path = obj.get("path", "")
                    if path.startswith("LFN:") or path.startswith("/"):
                        lfns.append(path)
                else:
                    for value in obj.values():
                        extract_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    extract_recursive(item)
        
        extract_recursive(job_order)
        return lfns
    
    def _filter_catalog(self, lfns: list[str]) -> ReplicaCatalog:
        """Filter master catalog for specific LFNs."""
        filtered_entries = {
            lfn: entry
            for lfn, entry in self.master_catalog.root.items()
            if lfn in lfns
        }
        return ReplicaCatalog(root=filtered_entries)
    
    def _update_catalog_with_outputs(
        self,
        process: Process,
        result: Tuple[Dict[str, Any] | None, str],
        runtime_context: RuntimeContext
    ):
        """Update master catalog with new output LFNs if needed."""

        # look at the outputs and see if any need to be propagated to our in-memory master catalog
        step_catalog_path = Path(runtime_context.outdir) / "replica_catalog.json"

        # Only update if a step catalog was created
        if not step_catalog_path.exists():
            logger.debug(f"No step catalog found at {step_catalog_path}, skipping update")
            return

        try:
            step_catalog = ReplicaCatalog.model_validate_json(
                step_catalog_path.read_text()
            )
            # Merge step catalog into master catalog
            if self.master_catalog is None:
                self.master_catalog = ReplicaCatalog(root={})
            for lfn, entry in step_catalog.root.items():
                self.master_catalog.root[lfn] = entry

            logger.info(f"Updated master catalog with {len(step_catalog.root)} entries from step")
        except Exception as e:
            logger.warning(f"Failed to update catalog from {step_catalog_path}: {e}")


def dirac_executor_factory(master_catalog_path: Path | None = None):
    """Factory function to create a DiracExecutor with configuration."""
    def executor(process, job_order, runtime_context, logger):
        dirac_exec = DiracExecutor(master_catalog_path)
        return dirac_exec(process, job_order, runtime_context, logger)
    return executor

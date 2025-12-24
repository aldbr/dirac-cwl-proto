# src/dirac_cwl_proto/cwltool_extensions/executor.py

import functools
import logging
from pathlib import Path
from typing import Any, Dict, List, cast

from cwltool.command_line_tool import CommandLineTool
from cwltool.context import RuntimeContext
from cwltool.errors import WorkflowException
from cwltool.executors import SingleJobExecutor
from cwltool.job import CommandLineJob
from cwltool.pathmapper import PathMapper
from cwltool.process import Process
from cwltool.stdfsaccess import StdFsAccess
from cwltool.utils import CWLObjectType
from cwltool.workflow import Workflow
from cwltool.workflow_job import WorkflowJob

from dirac_cwl_proto.executor.fs_access import DiracCatalogFsAccess
from dirac_cwl_proto.executor.pathmapper import DiracPathMapper
from dirac_cwl_proto.job.replica_catalog import ReplicaCatalog

logger = logging.getLogger("dirac-cwl-run")


# Monkey-patch CommandLineTool.make_path_mapper to respect runtime_context.path_mapper
_original_make_path_mapper = CommandLineTool.make_path_mapper


def _custom_make_path_mapper(
    reffiles: List[CWLObjectType],
    stagedir: str,
    runtimeContext: RuntimeContext,
    separateDirs: bool,
) -> PathMapper:
    """Custom make_path_mapper that uses runtime_context.path_mapper if available.

    This allows us to inject our custom DiracPathMapper into the workflow execution.
    """
    # Check if runtime_context has a custom path_mapper (like Toil does)
    if (
        hasattr(runtimeContext, "path_mapper")
        and runtimeContext.path_mapper != PathMapper
    ):
        # Use the custom path_mapper from runtime context
        return runtimeContext.path_mapper(
            reffiles, runtimeContext.basedir, stagedir, separateDirs
        )
    else:
        # Fall back to original implementation
        return _original_make_path_mapper(
            reffiles, stagedir, runtimeContext, separateDirs
        )


# Apply the monkey-patch
CommandLineTool.make_path_mapper = staticmethod(_custom_make_path_mapper)


class DiracExecutor(SingleJobExecutor):
    """Custom executor that handles replica catalog management between steps.

    This executor overrides run_jobs() to intercept each CommandLineJob execution
    and manage replica catalogs before and after the job runs.
    """

    def __init__(self, master_catalog_path: Path | None = None):
        super().__init__()
        self.master_catalog_path = master_catalog_path
        self.master_catalog: ReplicaCatalog | None = None

    def run_jobs(
        self,
        process: Process,
        job_order_object: Dict[str, Any],
        logger_arg: logging.Logger,
        runtime_context: RuntimeContext,
    ) -> None:
        """Override run_jobs to intercept each job execution.

        This method is called once at the top level and iterates through ALL jobs
        including nested CommandLineTools within subworkflows. The generator pattern
        flattens the workflow hierarchy so we see every CommandLineJob here.
        """

        # Load catalog once at the start
        if self.master_catalog is None:
            if self.master_catalog_path and self.master_catalog_path.exists():
                self.master_catalog = ReplicaCatalog.model_validate_json(
                    self.master_catalog_path.read_text()
                )
                logger.info(
                    f"Loaded catalog with {len(self.master_catalog.root)} file(s)"
                )
            else:
                self.master_catalog = ReplicaCatalog(root={})
                logger.debug("Initialized empty catalog")

        # Set up custom filesystem access that can resolve LFNs via replica catalog
        # we create a partial function that binds the replica catalog to our custom
        # fs access class
        runtime_context.make_fs_access = cast(
            type[StdFsAccess],
            functools.partial(
                DiracCatalogFsAccess, replica_catalog=self.master_catalog
            ),
        )

        # Set up custom path mapper that can resolve LFN: URIs using the replica catalog
        # This intercepts file objects BEFORE the default PathMapper strips the LFN: prefix
        runtime_context.path_mapper = functools.partial(  # type: ignore[assignment]
            DiracPathMapper,
            replica_catalog=self.master_catalog,
        )

        # Set up provenance for non-workflow processes (from SingleJobExecutor)
        if (
            not isinstance(process, Workflow)
            and runtime_context.research_obj is not None
        ):
            process.provenance_object = (
                runtime_context.research_obj.initialize_provenance(
                    full_name=runtime_context.cwl_full_name,
                    host_provenance=runtime_context.prov_host,
                    user_provenance=runtime_context.prov_user,
                    orcid=runtime_context.orcid,
                    run_uuid=runtime_context.research_obj.ro_uuid,
                    fsaccess=runtime_context.make_fs_access(""),
                )
            )
            process.parent_wf = process.provenance_object

        # Get job iterator - this yields ALL jobs including nested ones
        jobiter = process.job(job_order_object, self.output_callback, runtime_context)

        try:
            for job in jobiter:
                if job is not None:
                    # Standard setup from SingleJobExecutor.run_jobs
                    if runtime_context.builder is not None and hasattr(job, "builder"):
                        job.builder = runtime_context.builder
                    if job.outdir is not None:
                        self.output_dirs.add(job.outdir)

                    # Handle provenance (from SingleJobExecutor.run_jobs)
                    process_run_id: str | None = None
                    if runtime_context.research_obj is not None:
                        if not isinstance(process, Workflow):
                            prov_obj = process.provenance_object
                        else:
                            prov_obj = job.prov_obj
                        if prov_obj:
                            runtime_context.prov_obj = prov_obj
                            prov_obj.fsaccess = runtime_context.make_fs_access("")
                            prov_obj.evaluate(
                                process,
                                job,
                                job_order_object,
                                runtime_context.research_obj,
                            )
                            process_run_id = prov_obj.record_process_start(process, job)
                            runtime_context = runtime_context.copy()
                        runtime_context.process_run_id = process_run_id

                    # Validation mode (from SingleJobExecutor.run_jobs)
                    if runtime_context.validate_only is True:
                        if isinstance(job, WorkflowJob):
                            name = job.tool.lc.filename
                        else:
                            name = getattr(job, "name", str(job))
                        print(
                            f"{name} is valid CWL. No errors detected in the inputs.",
                            file=runtime_context.validate_stdout,
                        )
                        return

                    # CUSTOM: Intercept CommandLineJob to manage replica catalogs
                    if isinstance(job, CommandLineJob):
                        job_name = getattr(job, "name", "unknown")
                        self._prepare_job_catalog(job, runtime_context)

                    # Execute the job
                    job.run(runtime_context)

                    # CUSTOM: Update catalog after CommandLineJob completes
                    if isinstance(job, CommandLineJob):
                        self._update_catalog_from_job(job, runtime_context)
                else:
                    logger.error("Workflow cannot make any more progress.")
                    break
        except WorkflowException:
            raise
        except Exception as err:
            logger.exception("Got workflow error")
            raise WorkflowException(str(err)) from err

    def _prepare_job_catalog(
        self, job: CommandLineJob, runtime_context: RuntimeContext
    ):
        """Prepare replica catalog for a specific CommandLineJob.

        Args:
            job: The CommandLineJob about to be executed
            runtime_context: Runtime context containing execution settings
        """
        job_name = getattr(job, "name", "unknown")

        # Extract LFNs from job inputs
        # job.builder.job contains the actual input dictionary
        job_inputs = job.builder.job if hasattr(job, "builder") else {}
        step_lfns = self._extract_lfns_from_inputs(job_inputs)

        # Filter master catalog for this step's inputs
        if step_lfns and self.master_catalog:
            step_catalog = ReplicaCatalog(
                root={
                    lfn: entry
                    for lfn, entry in self.master_catalog.root.items()
                    if lfn in step_lfns
                }
            )
            found = len(step_catalog.root)
            if found > 0:
                logger.info(f"{job_name}: Found {found} input files in catalog")
            else:
                logger.warning(
                    f"{job_name}: Expected input files not found in catalog: {step_lfns}"
                )
                step_catalog = ReplicaCatalog(root={})
        elif step_lfns:
            logger.warning(
                f"{job_name}: Input files requested but no catalog available: {step_lfns}"
            )
            step_catalog = ReplicaCatalog(root={})
        else:
            # Create empty catalog for steps with no LFN inputs (e.g., simulation)
            step_catalog = ReplicaCatalog(root={})

        # Write step catalog to job's output directory
        if job.outdir:
            step_catalog_path = Path(job.outdir) / "replica_catalog.json"
            step_catalog_path.write_text(step_catalog.model_dump_json(indent=2))
        else:
            logger.warning(
                f"{job_name}: Job has no output directory, cannot write catalog"
            )

    def _update_catalog_from_job(
        self, job: CommandLineJob, runtime_context: RuntimeContext
    ):
        """Update catalog from job outputs.

        After a job completes, the lbprodrun wrapper may have added new LFNs
        to the step catalog. Merge those back into the catalog.

        Args:
            job: The completed CommandLineJob
            runtime_context: Runtime context containing execution settings
        """
        job_name = getattr(job, "name", "unknown")

        if not job.outdir:
            logger.warning(
                f"{job_name}: Job has no output directory, cannot update catalog"
            )
            return

        step_catalog_path = Path(job.outdir) / "replica_catalog.json"
        if not step_catalog_path.exists():
            logger.debug(f"{job_name}: No step catalog found, skipping update")
            return

        try:
            step_catalog = ReplicaCatalog.model_validate_json(
                step_catalog_path.read_text()
            )

            if self.master_catalog is None:
                self.master_catalog = ReplicaCatalog(root={})

            # Only register files that are NOT already in the master catalog
            # This filters out input files that were copied to the step catalog
            new_entries = []
            updated_entries = []
            for lfn, entry in step_catalog.root.items():
                if lfn in self.master_catalog.root:
                    # File already exists - check if it was updated
                    existing_entry = self.master_catalog.root[lfn]
                    # If the entry has changed (e.g., new replicas added), update it
                    if existing_entry != entry:
                        self.master_catalog.root[lfn] = entry
                        updated_entries.append(lfn)
                        logger.debug(f"{job_name}: Updated catalog entry for {lfn}")
                    # Otherwise skip - this is an input file that hasn't changed
                else:
                    # This is a new file (output from this job)
                    self.master_catalog.root[lfn] = entry
                    new_entries.append(lfn)

            if new_entries:
                logger.info(
                    f"{job_name}: Registered {len(new_entries)} new output file(s) (catalog total: {len(self.master_catalog.root)})"
                )
            if updated_entries:
                logger.info(
                    f"{job_name}: Updated {len(updated_entries)} existing catalog entries"
                )
        except Exception as e:
            logger.error(f"{job_name}: Failed to update catalog - {e}", exc_info=True)

    def _extract_lfns_from_inputs(self, job_order: Dict[str, Any]) -> list[str]:
        """Extract LFN paths from job inputs.

        Recursively searches through the job order dictionary to find File objects
        with paths that look like LFNs (start with "LFN:").

        Args:
            job_order: Job input dictionary

        Returns:
            List of LFN paths found in the inputs (with LFN: prefix stripped)
        """
        lfns = []

        def extract_recursive(obj):
            if isinstance(obj, dict):
                # Check if this looks like a File with an LFN path
                if "class" in obj and obj["class"] == "File":
                    # Check both "path" and "location" fields
                    for field in ["path", "location"]:
                        value = obj.get(field, "")
                        if value.startswith("LFN:"):
                            # Strip LFN: prefix for catalog lookup
                            lfn = value[4:]
                            if lfn not in lfns:
                                lfns.append(lfn)
                            break
                else:
                    for value in obj.values():
                        extract_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    extract_recursive(item)

        extract_recursive(job_order)
        return lfns


def dirac_executor_factory(master_catalog_path: Path | None = None):
    """Factory function to create a DiracExecutor with configuration.

    Args:
        master_catalog_path: Path to master replica catalog JSON file

    Returns:
        Executor function compatible with cwltool
    """

    def executor(process, job_order, runtime_context, logger_arg):
        dirac_exec = DiracExecutor(master_catalog_path)
        return dirac_exec(process, job_order, runtime_context, logger_arg)

    return executor

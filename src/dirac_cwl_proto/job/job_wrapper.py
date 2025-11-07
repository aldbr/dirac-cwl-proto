#!/usr/bin/env python

import logging
import random
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import cast

from cwl_utils.parser import (
    save,
)
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    File,
    Saveable,
    Workflow,
)
from rich.text import Text
from ruamel.yaml import YAML

from dirac_cwl_proto.execution_hooks.core import ExecutionHooksBasePlugin
from dirac_cwl_proto.submission_models import (
    JobInputModel,
    JobSubmissionModel,
)

# -----------------------------------------------------------------------------
# JobWrapper
# -----------------------------------------------------------------------------


class JobWrapper:
    """Job Wrapper for the execution hook."""

    def __download_input_sandbox(
        self, arguments: JobInputModel, job_path: Path
    ) -> None:
        """
        Download the files from the sandbox store
        """
        assert arguments.sandbox is not None
        for sandbox in arguments.sandbox:
            sandbox_path = Path("sandboxstore") / f"{sandbox}.tar.gz"
            with tarfile.open(sandbox_path, "r:gz") as tar:
                tar.extractall(job_path, filter="data")

    def __download_input_data(self, arguments: JobInputModel, job_path: Path) -> None:
        """
        Download input data
        """
        input_data = []
        for _, input_value in arguments.cwl.items():
            input = input_value
            if not isinstance(input_value, list):
                input = [input_value]

            for item in input:
                if not isinstance(item, File):
                    continue

                # TODO: path is not the only attribute to consider, but so far it is the only one used
                if not item.path:
                    raise NotImplementedError("File path is not defined.")

                input_path = Path(item.path)
                if "filecatalog" in input_path.parts:
                    input_data.append(item)

        for file in input_data:
            # TODO: path is not the only attribute to consider, but so far it is the only one used
            if not file.path:
                raise NotImplementedError("File path is not defined.")

            input_path = Path(file.path)
            shutil.copy(input_path, job_path / input_path.name)
            file.path = file.path.split("/")[-1]

    def _pre_process(
        self,
        executable: CommandLineTool | Workflow | ExpressionTool,
        arguments: JobInputModel | None,
        runtime_metadata: ExecutionHooksBasePlugin | None,
        job_path: Path,
    ) -> list[str]:
        """
        Pre-process the job before execution.

        :return: True if the job is pre-processed successfully, False otherwise
        """
        logger = logging.getLogger("JobWrapper - Pre-process")

        # Prepare the task for cwltool
        logger.info("Preparing the task for cwltool...")
        command = ["cwltool"]

        task_dict = save(executable)
        task_path = job_path / "task.cwl"
        with open(task_path, "w") as task_file:
            YAML().dump(task_dict, task_file)
        command.append(str(task_path.name))

        if arguments:
            if arguments.sandbox:
                # Download the files from the sandbox store
                logger.info("Downloading the files from the sandbox store...")
                self.__download_input_sandbox(arguments, job_path)
                logger.info("Files downloaded successfully!")

            # Download input data from the file catalog
            logger.info("Downloading input data from the file catalog...")
            self.__download_input_data(arguments, job_path)
            logger.info("Input data downloaded successfully!")

            # Prepare the parameters for cwltool
            logger.info("Preparing the parameters for cwltool...")
            parameter_dict = save(cast(Saveable, arguments.cwl))
            parameter_path = job_path / "parameter.cwl"
            with open(parameter_path, "w") as parameter_file:
                YAML().dump(parameter_dict, parameter_file)
            command.append(str(parameter_path.name))
        if runtime_metadata:
            return runtime_metadata.pre_process(job_path, command)

        return command

    def _post_process(
        self,
        status: int,
        stdout: str,
        stderr: str,
        job_path: Path,
        runtime_metadata: ExecutionHooksBasePlugin | None,
    ):
        """
        Post-process the job after execution.

        :return: True if the job is post-processed successfully, False otherwise
        """
        logger = logging.getLogger("JobWrapper - Post-process")
        if status != 0:
            raise RuntimeError(f"Error {status} during the task execution.")

        logger.info(stdout)
        logger.info(stderr)

        if runtime_metadata:
            return runtime_metadata.post_process(job_path)

        return True

    def run_job(self, job: JobSubmissionModel) -> bool:
        """
        Executes a given CWL workflow using cwltool.
        This is the equivalent of the DIRAC JobWrapper.

        :return: True if the job is executed successfully, False otherwise
        """
        logger = logging.getLogger("JobWrapper")
        # Instantiate runtime metadata from the serializable descriptor and
        # the job context so implementations can access task inputs/overrides.
        runtime_metadata = (
            job.execution_hooks.to_runtime(job) if job.execution_hooks else None
        )

        # Isolate the job in a specific directory
        job_path = Path(".") / "workernode" / f"{random.randint(1000, 9999)}"
        job_path.mkdir(parents=True, exist_ok=True)

        try:
            # Pre-process the job
            logger.info("Pre-processing Task...")
            command = self._pre_process(
                job.task,
                job.parameters[0] if job.parameters else None,
                runtime_metadata,
                job_path,
            )
            logger.info("Task pre-processed successfully!")

            # Execute the task
            logger.info(f"Executing Task: {command}")
            result = subprocess.run(
                command, capture_output=True, text=True, cwd=job_path
            )

            if result.returncode != 0:
                logger.error(
                    f"Error in executing workflow:\n{Text.from_ansi(result.stderr)}"
                )
                return False
            logger.info("Task executed successfully!")

            # Post-process the job
            logger.info("Post-processing Task...")
            if self._post_process(
                result.returncode,
                result.stdout,
                result.stderr,
                job_path,
                runtime_metadata,
            ):
                logger.info("Task post-processed successfully!")
                return True
            logger.error("Failed to post-process Task")
            return False

        except Exception:
            logger.exception("JobWrapper: Failed to execute workflow")
            return False
        finally:
            # Clean up
            if job_path.exists():
                shutil.rmtree(job_path)

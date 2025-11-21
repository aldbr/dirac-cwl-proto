import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


class CommandBase(ABC):
    """Base abstract class for pre/post-processing tasks.

    New commands should inherit this class and implement the 'execute' function.

    This commands could be programmed to be executed only in one stage (pre-process or post-process) or in both stages.
    For example, a command could write in a file when it got called. This command could be executed both at the
    pre-processing and post-processing stages marking the beginning and end of the job executed.
    """

    @abstractmethod
    def execute(self, job_path: Path, **kwargs) -> None:
        raise NotImplementedError()


class JobTypeProcessorBase:
    """Base class for processing groups of commands during pre-processing and post-processing stages.

    Job types should inherit this class and modify ONLY the lists of pre-processing and post-processing commands.
    The commands MUST be in the desired order of execution.

    The lists contain command TYPES, not instances.
    """

    preprocess_commands: List[type[CommandBase]] = []
    postprocess_commands: List[type[CommandBase]] = []

    @classmethod
    def pre_process(cls, job_path: Path, **kwargs) -> None:
        for command in cls.preprocess_commands:
            try:
                command().execute(job_path, **kwargs)
            except Exception as e:
                logger.error(
                    f"Command '{command.__name__}' failed during the pre-process stage: {e}"
                )
                raise

    @classmethod
    def post_process(cls, job_path: Path, **kwargs) -> None:
        for command in cls.postprocess_commands:
            try:
                command().execute(job_path, **kwargs)
            except Exception as e:
                logger.error(
                    f"Command '{command.__name__}' failed during the post-process stage: {e}"
                )
                raise

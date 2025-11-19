from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

# ! This creates a circular import
# This happens because The JobProcessor requires info from the JobWrapper,
#  which needs info from the plugins and hints ...
# from dirac_cwl_proto.job.job_wrapper import JobWrapper


class CommandBase(ABC):
    @abstractmethod
    def execute(self, job_wrapper):
        raise NotImplementedError()


class JobProcessorBase:
    preprocess_commands: List[type[CommandBase]] = []
    postprocess_commands: List[type[CommandBase]] = []

    @classmethod
    def __init__(cls, jobWrapper):
        cls.jobWrapper = jobWrapper

    @classmethod
    def pre_process(cls):
        for command in cls.preprocess_commands:
            command().execute(cls.jobWrapper)

    @classmethod
    def post_process(cls):
        for command in cls.postprocess_commands:
            command().execute(cls.jobWrapper)

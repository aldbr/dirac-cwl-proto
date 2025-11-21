from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List


class CommandBase(ABC):
    @abstractmethod
    def execute(self, job_path, **kwargs):
        raise NotImplementedError()


class JobProcessorBase:
    preprocess_commands: List[type[CommandBase]] = []
    postprocess_commands: List[type[CommandBase]] = []

    @classmethod
    def pre_process(cls, job_path, **kwargs):
        for command in cls.preprocess_commands:
            command().execute(job_path, **kwargs)

    @classmethod
    def post_process(cls, job_path, **kwargs):
        for command in cls.postprocess_commands:
            command().execute(job_path, **kwargs)

from dirac_cwl_proto.commands import JobProcessorBase

from .download_config import DownloadConfig
from .group_outputs import GroupOutputs


class TestingJobType(JobProcessorBase):
    preprocess_commands = [DownloadConfig]
    postprocess_commands = [GroupOutputs]

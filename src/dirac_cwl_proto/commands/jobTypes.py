from dirac_cwl_proto.commands import JobProcessorBase

from .downloadConfig import DownloadConfig
from .groupOutputs import GroupOutputs


class TestingJobType(JobProcessorBase):
    preprocess_commands = [DownloadConfig]
    postprocess_commands = [GroupOutputs]

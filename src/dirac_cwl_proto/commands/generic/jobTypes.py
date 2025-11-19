from dirac_cwl_proto.commands import JobProcessorBase

from .modules import DownloadConfig, GroupOutputs


class TestingJobType(JobProcessorBase):
    preprocess_commands = [DownloadConfig]
    postprocess_commands = [GroupOutputs]

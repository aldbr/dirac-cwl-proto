from dirac_cwl_proto.commands import JobTypeProcessorBase

from .download_config import DownloadConfig
from .group_outputs import GroupOutputs


class TestingJobType(JobTypeProcessorBase):
    """JobType example.
    Jobs with this type will launch 'DownloadConfig.execute' during their pre-processing stage and
    'GroupOutputs.execute' during their post-processing stage.
    """

    preprocess_commands = [DownloadConfig]
    postprocess_commands = [GroupOutputs]

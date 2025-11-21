import os

from dirac_cwl_proto.commands import CommandBase


class DownloadConfig(CommandBase):
    """Example command that creates a file with named 'content.cfg'.

    This command is expected to be executed during the pre-processing stage.
    """

    def execute(self, job_path, **kwargs):
        content = """\
This is an example
"""
        filePath = os.path.join(job_path, "content.cfg")
        with open(filePath, "w") as f:
            f.write(content)

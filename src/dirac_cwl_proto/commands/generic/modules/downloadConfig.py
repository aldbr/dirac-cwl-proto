import os

from dirac_cwl_proto.commands import CommandBase
from dirac_cwl_proto.job.job_wrapper import JobWrapper


class DownloadConfig(CommandBase):
    def execute(self, job_wrapper: JobWrapper):
        content = """\
This is an example
"""
        filePath = os.path.join(job_wrapper.job_path, "content.cfg")
        with open(filePath, "w") as f:
            f.write(content)

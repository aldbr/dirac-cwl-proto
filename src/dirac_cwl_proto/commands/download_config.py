import os

from dirac_cwl_proto.commands import PreProcessCommand


class DownloadConfig(PreProcessCommand):
    """Example command that creates a file with named 'content.cfg'."""

    def execute(self, job_path, **kwargs):
        content = """\
This is an example
"""
        file_path = os.path.join(job_path, "content.cfg")
        with open(file_path, "w") as f:
            f.write(content)

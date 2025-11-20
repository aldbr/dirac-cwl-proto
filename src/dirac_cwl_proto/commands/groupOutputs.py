import glob
import os

from dirac_cwl_proto.commands import CommandBase
from dirac_cwl_proto.job.job_wrapper import JobWrapper


class GroupOutputs(CommandBase):
    def execute(self, job_wrapper: JobWrapper):
        groupedOutputs = "group.out"
        outputPath = os.path.join(job_wrapper.job_path, groupedOutputs)
        outputFiles = ["*.out", "*.txt"]

        with open(outputPath, "w", encoding="utf-8") as fIn:
            for fileType in outputFiles:
                extension = f"{job_wrapper.job_path}/{fileType}"
                for file in glob.glob(extension):
                    if file == outputPath:
                        continue

                    with open(file, "r", encoding="utf-8") as fOut:
                        fIn.write(f"############ {file}\n")
                        fIn.writelines(fOut.readlines())
                        fIn.write("\n")

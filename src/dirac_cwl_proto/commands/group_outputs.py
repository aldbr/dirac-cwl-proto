import glob
import os

from dirac_cwl_proto.commands import CommandBase


class GroupOutputs(CommandBase):
    def execute(self, job_path, **kwargs):
        groupedOutputs = "group.out"
        outputPath = os.path.join(job_path, groupedOutputs)
        outputFiles = ["*.out", "*.txt"]

        with open(outputPath, "w", encoding="utf-8") as fIn:
            for fileType in outputFiles:
                extension = f"{job_path}/{fileType}"
                for file in glob.glob(extension):
                    if file == outputPath:
                        continue

                    with open(file, "r", encoding="utf-8") as fOut:
                        fIn.write(f"############ {file}\n")
                        fIn.writelines(fOut.readlines())
                        fIn.write("\n")

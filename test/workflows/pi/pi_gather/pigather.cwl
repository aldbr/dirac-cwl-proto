cwlVersion: v1.1
class: CommandLineTool

requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 1024

inputs:
  input-data:
    type: File
    inputBinding:
      separate: true

outputs:
  pi_result:
    type: File
    outputBinding:
      glob: "output.pi"

baseCommand: [pi_gather]

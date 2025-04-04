cwlVersion: v1.2
class: CommandLineTool

requirements:
  ResourceRequirement:
    coresMin: 2
    ramMin: 1024

inputs:
  num-points:
    type: int
    default: 1000
    inputBinding:
      position: 1

outputs:
  result_sim:
    type: File[]
    outputBinding:
      glob: "result*.sim"

baseCommand: [pi-simulate]

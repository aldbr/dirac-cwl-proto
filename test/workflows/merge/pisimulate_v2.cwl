cwlVersion: v1.2
class: CommandLineTool

requirements:
  ResourceRequirement:
    coresMin: 2
    ramMin: 1024

inputs:
  num-points:
    type: int
    default: 100
    inputBinding:
      position: 1
  output-path:
    type: string
    default: result_3.sim
    inputBinding:
      position: 2

outputs:
  sim:
    type: File
    outputBinding:
      glob: "result*.sim"

baseCommand: [pi-simulate-v2]

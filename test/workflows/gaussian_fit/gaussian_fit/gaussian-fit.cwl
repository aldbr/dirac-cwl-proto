cwlVersion: v1.2
class: CommandLineTool
label: "Gaussian Fit Tool"

inputs:
  data:
    type: File[]
    inputBinding:
      position: 1

outputs:
  fit-data:
    type: File[]
    outputBinding:
      glob: ["fit.txt"]
  log:
    type: File[]
    outputBinding:
      glob: ["fit.log"]

baseCommand: ["gaussian-fit"]

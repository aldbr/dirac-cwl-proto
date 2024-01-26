baseCommand:
- python
- process.py
class: CommandLineTool
cwlVersion: v1.2
inputs:
  input-data: File
outputs:
  results:
    outputBinding:
      glob: '*'
    type: File[]
requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 2048

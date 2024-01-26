baseCommand:
- python
- simulate.py
class: CommandLineTool
cwlVersion: v1.2
inputs:
  simulation-config: File
outputs:
  result.sim:
    type: File
requirements:
  ResourceRequirement:
    coresMin: 4
    ramMin: 2048

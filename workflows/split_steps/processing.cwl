baseCommand: processing.py
class: CommandLineTool
inputs:
  files: File[]
outputBinding:
  glob: '*'
requirements:
  ResourceRequirement:
    coresMin: 2
    ramMin: 2048

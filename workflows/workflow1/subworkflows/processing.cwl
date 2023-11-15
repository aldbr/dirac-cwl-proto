baseCommand:
- python
- ../src/dirac_cwl_proto/modules/process.py
class: CommandLineTool
cwlVersion: v1.2
inputs:
  input_data: File[]
outputs:
  results:
    outputBinding:
      glob: '*'
    type: File[]
requirements:
  ResourceRequirement:
    coresMin: 4
    ramMin: 2048

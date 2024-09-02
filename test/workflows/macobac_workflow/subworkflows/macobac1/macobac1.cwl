class: CommandLineTool
inputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/macobac_workflow/description.cwl#macobac1/run/configuration
  type: File
  inputBinding:
    prefix: --config
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/macobac_workflow/description.cwl#macobac1/run/log-level
  type:
  - 'null'
  - string
  inputBinding:
    prefix: --log-level
outputs: []
requirements:
- class: ResourceRequirement
  coresMin: 4
  ramMin: 2048
cwlVersion: v1.2
baseCommand:
- calculate_macobac.py

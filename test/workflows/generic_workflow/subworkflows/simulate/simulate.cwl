class: CommandLineTool
inputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/generic_workflow/description.cwl#simulate/run/max-random
  type: int
  inputBinding:
    position: 1
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/generic_workflow/description.cwl#simulate/run/min-random
  type: int
  inputBinding:
    position: 2
outputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/generic_workflow/description.cwl#simulate/run/result_sim
  type:
    name: _:258b756a-1569-44f3-b828-cf3fffe604b0
    items: File
    type: array
  outputBinding:
    glob: "*result.sim"
requirements:
- class: ResourceRequirement
  coresMin: 4
  ramMin: 2048
cwlVersion: v1.2
baseCommand:
- generic-simulate

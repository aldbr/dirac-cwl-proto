class: CommandLineTool
inputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/generic_workflow/description.cwl#processing/run/input-data
  type:
    name: _:116cf272-cfc0-49f2-a61f-0bb5ce5d5615
    items: File
    type: array
  inputBinding:
    separate: true
outputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/generic_workflow/description.cwl#processing/run/results
  type:
    name: _:13b4c769-fed6-4223-999b-a4a9e6294f4c
    items: File
    type: array
  outputBinding:
    glob: "output.dst"
requirements:
- class: ResourceRequirement
  coresMin: 1
  ramMin: 2048
cwlVersion: v1.2
baseCommand:
- generic-process

class: Workflow
inputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/max-iterations
  type: int
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/number-of-lines
  type: int
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/precision
  type: float
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/start-line
  type: int
outputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/data
  outputSource: image-prod/run/run-mandelbrot/data
  type:
  - 'null'
  - name: _:c8a1b18d-0537-4f7a-a33c-f62b3e06b839
    items: File
    type: array
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/log
  outputSource: image-prod/run/run-mandelbrot/log
  type:
  - 'null'
  - name: _:6b07db8b-ab53-478b-b8a0-5c7e871be112
    items: File
    type: array
requirements:
- class: ResourceRequirement
  coresMin: 1
  coresMax: 4
  ramMin: 1024
  ramMax: 4096
cwlVersion: v1.2
steps:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/get-mandelbrot
  in: []
  out:
  - repo
  run:
    class: CommandLineTool
    id: _:7835fd09-6145-48a3-902b-181742833ef8
    inputs: []
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/get-mandelbrot/run/repo
      type: Directory
      outputBinding:
        glob: "mandel4ts"
    baseCommand:
    - "git"
    - "clone"
    - "https://gitlab.cta-observatory.org/arrabito/mandel4ts.git"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/run-mandelbrot
  in:
  - id: max-iterations
    source: image-prod/run/max-iterations
  - id: number-of-lines
    source: image-prod/run/number-of-lines
  - id: precision
    source: image-prod/run/precision
  - id: repo
    source: image-prod/run/get-mandelbrot/repo
  - id: start-line
    source: image-prod/run/start-line
  out:
  - data
  - log
  run:
    class: CommandLineTool
    id: _:d12b10ec-4fb3-419c-a98e-eebbff2ba9b6
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/run-mandelbrot/run/max-iterations
      type: int
      inputBinding:
        prefix: "-M"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/run-mandelbrot/run/number-of-lines
      type: int
      inputBinding:
        prefix: "-N"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/run-mandelbrot/run/precision
      type: float
      inputBinding:
        prefix: "-P"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/run-mandelbrot/run/repo
      type: Directory
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/run-mandelbrot/run/start-line
      type: int
      inputBinding:
        prefix: "-L"
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/run-mandelbrot/run/data
      type:
      - 'null'
      - name: _:ea7497eb-211c-4b7c-8f62-6bea46cfe0a4
        items: File
        type: array
      outputBinding:
        glob: "data*txt"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-prod/run/run-mandelbrot/run/log
      type:
      - 'null'
      - name: _:922f410e-8539-4552-b335-bb9e6fbd59cc
        items: File
        type: array
      outputBinding:
        glob:
        - "*log"
    requirements:
    - class: InitialWorkDirRequirement
      listing:
      - $(inputs.repo)
    baseCommand:
    - "python"
    - "mandel4ts/mandelbrot.py"

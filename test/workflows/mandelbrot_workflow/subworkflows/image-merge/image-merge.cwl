class: Workflow
inputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/data
  type:
  - 'null'
  - name: _:9911fbfd-4ef1-4e0c-82b4-29fc2f5fdb85
    items: File
    type: array
outputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/data-merged
  outputSource: image-merge/run/merge-mandelbrot/data-merged
  type:
  - 'null'
  - name: _:f8c4bde7-289a-4e39-94ae-5d4b55f94d75
    items: File
    type: array
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/log
  outputSource: image-merge/run/merge-mandelbrot/log
  type:
  - 'null'
  - name: _:24d00f63-e4d8-4f67-89f7-8e86f61043bb
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
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/get-mandelbrot
  in: []
  out:
  - repo
  run:
    class: CommandLineTool
    id: _:d34fc922-280c-475d-a450-5b14ca87116f
    inputs: []
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/get-mandelbrot/run/repo
      type: Directory
      outputBinding:
        glob: "mandel4ts"
    baseCommand:
    - "git"
    - "clone"
    - "https://gitlab.cta-observatory.org/arrabito/mandel4ts.git"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/merge-mandelbrot
  in:
  - id: data
    source: image-merge/run/data
  - id: repo
    source: image-merge/run/get-mandelbrot/repo
  out:
  - data-merged
  - log
  run:
    class: CommandLineTool
    id: _:33bc7439-40e0-4f9b-9f03-2789fb242257
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/merge-mandelbrot/run/data
      type:
      - 'null'
      - name: _:cf24eec5-501a-4bb7-b072-b93fc6e0dc3f
        items: File
        type: array
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/merge-mandelbrot/run/repo
      type: Directory
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/merge-mandelbrot/run/data-merged
      type:
      - 'null'
      - name: _:03f4c3f2-6c57-406a-9923-2b0abc935e7f
        items: File
        type: array
      outputBinding:
        glob:
        - "data_merged*txt"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/mandelbrot_workflow/description.cwl#image-merge/run/merge-mandelbrot/run/log
      type:
      - 'null'
      - name: _:863b6178-e7a4-4585-bd13-8e398440d24d
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
    - "./mandel4ts/merge_data.py"

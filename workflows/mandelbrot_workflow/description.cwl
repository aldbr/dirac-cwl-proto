cwlVersion: v1.2
class: Workflow
label: "Mandelbrot Workflow"
doc: >
  This workflow is composed of 2 Workflow steps:
  - image-prod: produce an image
  - image-merge: merge the images produced by several image-prod

# Define the inputs of the workflow
inputs:
  precision:
    type: float
  max-iterations:
    type: int
  start-line:
    type: int
  number-of-lines:
    type: int

# Define the outputs of the workflow
outputs:
  merged-data:
    type: File[]?
    outputSource: image-merge/data-merged
  logs:
    type: File[]?
    outputSource:
      - image-prod/log
      - image-merge/log
    linkMerge: merge_flattened


# Requirements for the workflow
requirements:
  SubworkflowFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}

# Define the steps of the workflow
steps:
  # Producing the image
  image-prod:
    in:
      precision: precision
      max-iterations: max-iterations
      start-line: start-line
      number-of-lines: number-of-lines
    out:
      - data
      - log
    run:
      class: Workflow
      requirements:
        ResourceRequirement:
          coresMin: 1
          coresMax: 4
          ramMin: 1024
          ramMax: 4096

      inputs:
        precision:
          type: float
        max-iterations:
          type: int
        start-line:
          type: int
        number-of-lines:
          type: int

      outputs:
        data:
          type: File[]?
          outputSource: run-mandelbrot/data
        log:
          type: File[]?
          outputSource: run-mandelbrot/log

      steps:
        # Clone the project
        get-mandelbrot:
          in: []
          out: [repo]
          run:
            class: CommandLineTool
            baseCommand: ["git", "clone", "https://gitlab.cta-observatory.org/arrabito/mandel4ts.git"]
            inputs: []
            outputs:
              repo:
                type: Directory
                outputBinding:
                  glob: "mandel4ts"

        # Run mandelbrot
        run-mandelbrot:
          in:
            precision: precision
            max-iterations: max-iterations
            start-line: start-line
            number-of-lines: number-of-lines
            repo: get-mandelbrot/repo
          out: [data, log]
          run:
            class: CommandLineTool
            baseCommand: ["python", "mandel4ts/mandelbrot.py"]
            requirements:
              InitialWorkDirRequirement:
                listing:
                  - $(inputs.repo)  # Stage the repo directory

            inputs:
              precision:
                type: float
                inputBinding:
                  prefix: "-P"
              max-iterations:
                type: int
                inputBinding:
                  prefix: "-M"
              start-line:
                type: int
                inputBinding:
                  prefix: "-L"
              number-of-lines:
                type: int
                inputBinding:
                  prefix: "-N"
              repo:
                type: Directory

            outputs:
              data:
                type: File[]?
                outputBinding:
                  glob: "data*txt"
              log:
                type: File[]?
                outputBinding:
                  glob: ["*log"]

  # Merge the images
  image-merge:
    in:
      data: image-prod/data
    out:
      - data-merged
      - log
    run:
      class: Workflow
      requirements:
        ResourceRequirement:
          coresMin: 1
          coresMax: 4
          ramMin: 1024
          ramMax: 4096

      inputs:
        data:
          type: File[]?

      outputs:
        data-merged:
          type: File[]?
          outputSource: merge-mandelbrot/data-merged
        log:
          type: File[]?
          outputSource: merge-mandelbrot/log

      steps:
        # Clone the project
        get-mandelbrot:
          in: []
          out: [repo]
          run:
            class: CommandLineTool
            baseCommand: ["git", "clone", "https://gitlab.cta-observatory.org/arrabito/mandel4ts.git"]
            inputs: []
            outputs:
              repo:
                type: Directory
                outputBinding:
                  glob: "mandel4ts"

        merge-mandelbrot:
          in:
            data: data
            repo: get-mandelbrot/repo
          out: [data-merged, log]
          run:
            class: CommandLineTool
            baseCommand: ["python", "./mandel4ts/merge_data.py"]
            requirements:
              InitialWorkDirRequirement:
                listing:
                  - $(inputs.repo)  # Stage the repo directory

            inputs:
              data:
                type: File[]?
              repo:
                type: Directory

            outputs:
              data-merged:
                type: File[]?
                outputBinding:
                  glob: ["data_merged*txt"]
              log:
                type: File[]?
                outputBinding:
                  glob: ["*log"]

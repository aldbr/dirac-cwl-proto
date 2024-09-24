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
    default: 0.0005
  max_iterations:
    type: int
    default: 1000
  start_x:
    type: float
    default: -0.5
  start_y:
    type: float
    default: 0.0
  step:
    type: int
    default: 1
  split:
    type: int
    default: 1
  width:
    type: int
    default: 7680
  height:
    type: int
    default: 4320
  output_name:
    type: string
    default: "data"

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
      max_iterations: max_iterations
      start_x: start_x
      start_y: start_y
      step: step
      split: split
      width: width
      height: height
      output_name: output_name
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
        max_iterations:
          type: int
        start_x:
          type: float
        start_y:
          type: float
        step:
          type: int
        split:
          type: int
        width:
          type: int
        height:
          type: int
        output_name:
          type: string
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
            max_iterations: max_iterations
            start_x: start_x
            start_y: start_y
            step: step
            split: split
            width: width
            height: height
            output_name: output_name
            repo: get-mandelbrot/repo
          out: [data, log]
          run:
            class: CommandLineTool
            baseCommand: ["python", "mandel4ts/src/mandel4ts/mandelbrot_generator.py"]
            requirements:
              InitialWorkDirRequirement:
                listing:
                  - $(inputs.repo)  # Stage the repo directory

            inputs:
              precision:
                type: float
                inputBinding:
                  prefix: "--precision"
              max_iterations:
                type: int
                inputBinding:
                  prefix: "--max-iterations"
              step:
                type: int
                inputBinding:
                  prefix: "--step"
              split:
                type: int
                inputBinding:
                  prefix: "--split"
              start_x:
                type: float
                inputBinding:
                  prefix: "--cx"
              start_y:
                type: float
                inputBinding:
                  prefix: "--cy"
              width:
                type: int
                inputBinding:
                  prefix: "--width"
              height:
                type: int
                inputBinding:
                  prefix: "--height"
              output_name:
                type: string
                inputBinding:
                  position: 9
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
            baseCommand: ["python", "./mandel4ts/src/mandel4ts/create_bitmap_image.py"]
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
                  glob: ["mandelbrot_image*.bmp"]
              log:
                type: File[]?
                outputBinding:
                  glob: ["*log"]

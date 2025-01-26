cwlVersion: v1.2
label: "Mandelbrot Workflow - Image Prod step"
class: Workflow

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
    default: 3
  split:
    type: int
    default: 3
  width:
    type: int
    default: 1920
  height:
    type: int
    default: 1080
  output_name:
    type: string
    default: data_

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

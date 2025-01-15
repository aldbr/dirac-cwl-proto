cwlVersion: v1.2
label: "Benchmark Data Generation Workflow"
class: Workflow

inputs:
  random_seed:
    type: float
    default: 0.0005

outputs:
  data:
    type: File[]?
    outputSource: 
      - gen-data-1/data
      - gen-data-2/data
  log:
    type: File[]?
    outputSource: 
      - gen-data-1/log
      - gen-data-2/log

steps:
  # Generate first set of data
  gen-data-1:
    in:
      random_seed: random_seed
    out: [data, log]
    run:
      class: CommandLineTool
      baseCommand: ["python", "generate_data.py"]
      inputs:
        random_seed:
          type: float
          inputBinding:
            prefix: "--random_seed"
      outputs:
        data:
          type: File[]?
          outputBinding:
            glob: "data.txt"
        log:
          type: File[]?
          outputBinding:
            glob: ["gen.log"]

  # Generate second set of data
  gen-data-2:
    in:
      random_seed: random_seed
    out: [data, log]
    run:
      class: CommandLineTool
      baseCommand: ["python", "generate_data.py"]
      requirements:
        InitialWorkDirRequirement: []
      inputs:
        random_seed:
          type: float
          inputBinding:
            prefix: "--random_seed"
      outputs:
        data:
          type: File[]?
          outputBinding:
            glob: "data.txt"
        log:
          type: File[]?
          outputBinding:
            glob: ["gen.log"]

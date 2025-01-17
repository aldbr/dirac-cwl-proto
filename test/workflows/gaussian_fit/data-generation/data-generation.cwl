cwlVersion: v1.2
label: "Benchmark Data Generation Workflow"
class: Workflow

inputs:
  output_file:
    type: string
    default: data.txt

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
      output_file: output_file
    out: [data, log]
    run:
      class: CommandLineTool
      baseCommand: ["random-data-gen"]
      inputs:
        output_file:
          type: float
          inputBinding:
            prefix: "--output_file"
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
      output_file: output_file
    out: [data, log]
    run:
      class: CommandLineTool
      baseCommand: ["random-data-gen"]
      requirements:
        InitialWorkDirRequirement: []
      inputs:
        output_file:
          type: float
          inputBinding:
            prefix: "--output_file"
      outputs:
        data:
          type: File[]?
          outputBinding:
            glob: "data.txt"
        log:
          type: File[]?
          outputBinding:
            glob: ["gen.log"]

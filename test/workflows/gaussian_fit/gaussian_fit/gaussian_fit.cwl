cwlVersion: v1.2
label: "Fit execution Workflow"
class: Workflow

inputs:
  data:
    type: File[]?

outputs:
  fit-data:
    type: File[]?
    outputSource: 
      - run-fit-1/fit-data
      - run-fit-2/fit-data
  log:
    type: File[]?
    outputSource:
      - run-fit-1/log
      - run-fit-2/log

steps:
  # Run fit 1
  run-fit-1:
    in: 
      data: data
    out: [fit-1-data, fit-1-log]
    run:
      class: CommandLineTool
      baseCommand: ["gaussian-fit"]
      inputs:
        data:
          type: File[]?
  
      outputs:
        fit-data:
          type: File[]?
          outputBinding:
            glob: ["fit.txt"]
        log:
          type: File[]?
          outputBinding:
            glob: ["*log"]

  # Run fit 2
  run-fit-2:
    in:
      data: data
    out: [fit-2-data, fit-2-log]
    run:
      class: CommandLineTool
      baseCommand: ["gaussian-fit"]
      requirements:
        InitialWorkDirRequirement:
          listing:
            - $(inputs.data)  # Stage the data files

      inputs:
        data:
          type: File[]?

      outputs:
        fit-data:
          type: File[]?
          outputBinding:
            glob: ["fit.txt"]
        log:
          type: File[]?
          outputBinding:
            glob: ["*log"]

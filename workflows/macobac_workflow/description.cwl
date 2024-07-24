cwlVersion: v1.2
class: Workflow
label: "Macobac Workflow"
doc: >
  This workflow is composed of two independent CommandLineTool steps that can be executed in parallel.

# Define the inputs of the workflow
inputs:
  # Input data
  configuration:
    type: File
  log-level:
    type: string?

outputs: []

# Define the steps of the workflow
steps:
  macobac1:
    in:
      configuration: configuration
      log-level: log-level
    out: []
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 4
          ramMin: 2048
      inputs:
        configuration:
          type: File
          inputBinding:
            prefix: --config
        log-level:
          type: string?
          inputBinding:
            prefix: --log-level
      outputs: []
      baseCommand: [calculate_macobac.py]

  macobac2:
    in:
      configuration: configuration
      log-level: log-level
    out: []
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 4
          ramMin: 2048
      inputs:
        configuration:
          type: File
          inputBinding:
            prefix: --config
        log-level:
          type: string?
          inputBinding:
            prefix: --log-level
      outputs: []
      baseCommand: [calculate_macobac.py]

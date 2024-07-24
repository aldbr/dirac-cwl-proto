cwlVersion: v1.2
class: Workflow
label: "My Analysis Workflow"
doc: >
  This workflow processes data through 2 CommandLineTool steps:
  - simulation: generate some data
  - processing: use the generated data to produce some results

# Define the inputs of the workflow
inputs:
  # Input data: simulation parameters
  max-random:
    type: int
  min-random:
    type: int

# Define the outputs of the workflow
outputs:
  processing_results:
    type: File[]
    outputSource: processing/results

# Define the steps of the workflow
steps:
  # Simulation step
  simulate:
    in:
      max-random: max-random
      min-random: min-random
    out: [result_sim]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 4
          ramMin: 2048
      inputs:
        max-random:
          type: int
          inputBinding:
            position: 1
        min-random:
          type: int
          inputBinding:
            position: 2
      outputs:
        result_sim:
          type: File[]
          outputBinding:
            glob: "*result.sim"
      baseCommand: [generic_simulate.py]

  # Processing step
  processing:
    in:
      input-data:
        source: simulate/result_sim
    out: [results]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 1
          ramMin: 2048
      inputs:
        input-data:
          type: File[]
          inputBinding:
            separate: true
      outputs:
        results:
          type: File[]
          outputBinding:
            glob: "output.dst"
      baseCommand: [generic_process.py]

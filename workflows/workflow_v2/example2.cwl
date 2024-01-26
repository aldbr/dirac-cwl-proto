cwlVersion: v1.2
class: Workflow
label: "My Analysis Workflow"
doc: >
  This workflow processes data through several steps including
  simulation, data resolution, and processing. It's designed for
  XYZ analysis.

# Define the inputs for the workflow
inputs:
  min-random: int  # Parameters for the simulation step
  max-random: int  # Parameters for the simulation step

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
      min-random: min-random
      max-random: max-random
    out: [result.sim]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 4
          ramMin: 2048
      inputs:
        min-random: int
        max-random: int
      outputs:
        result.sim:
          type: File
      baseCommand: [python, simulate.py]

  # Processing step
  processing:
    in:
      input-data:
        source: simulate/result.sim
    out: [results]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 1
          ramMin: 2048
      inputs:
        input-data: File
      outputs:
        results:
          type: File[]
          outputBinding:
            glob: "*"
      baseCommand: [python, process.py]

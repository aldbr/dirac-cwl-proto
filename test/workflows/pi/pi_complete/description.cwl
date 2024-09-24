cwlVersion: v1.2
class: Workflow
label: "Monte Carlo Pi Approximation Workflow"
doc: >
  This workflow approximates the value of Pi using the Monte Carlo method.
  It generates random points in a square and calculates how many fall within
  a unit circle inscribed in the square.

# Define the inputs of the workflow
inputs:
  num-points:
    type: int
    doc: "Number of random points to generate for the simulation"

# Define the outputs of the workflow
outputs:
  pi_approximation:
    type: File
    outputSource: processing/pi_result

# Define the steps of the workflow
steps:
  # Simulation step
  simulate:
    in:
      num-points: num-points
    out: [result_sim]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 2
          ramMin: 1024
      inputs:
        num-points:
          type: int
          inputBinding:
            position: 1
      outputs:
        result_sim:
          type: File
          outputBinding:
            glob: "*result.sim"
      baseCommand: [pi_simulate]

  # Processing step
  processing:
    in:
      input-data:
        source: simulate/result_sim
    out: [pi_result]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 1
          ramMin: 1024
      inputs:
        input-data:
          type: File
          inputBinding:
            separate: true
      outputs:
        pi_result:
          type: File
          outputBinding:
            glob: "output.pi"
      baseCommand: [pi_gather]

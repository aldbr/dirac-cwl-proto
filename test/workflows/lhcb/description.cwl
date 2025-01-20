cwlVersion: v1.2
class: Workflow
label: "LHCb MC workflow"
doc: >
  This workflow is composed of 2 main steps that should generate 2 types of jobs:
  * MCSimulation (CommandLineTool): Gauss execution
  * MCReconstruction (Workflow): Boole, Moore, Brunel and DaVinci executions based on Gauss outputs

# Define the inputs of the workflow
inputs:
  run-id:
    type: int
    default: 123
  task-id:
    type: int
    default: 456

# Define the outputs of the workflow
outputs:
  simulation_results:
    type: File[]?
    outputSource: simulation/sim
  simulation_others:
    type: File[]?
    outputSource: simulation/others
  reconstruction_results:
    type: File[]?
    outputSource: reconstruction/results
  reconstruction_others:
    type: File[]?
    outputSource: reconstruction/others

# Requirements for the workflow
requirements:
  SubworkflowFeatureRequirement: {}

# Define the steps of the workflow
steps:
  # Simulation step
  simulation:
    in:
      run-id: run-id
      task-id: task-id
    out: [sim, pool_xml_catalog, others]
    run: ./lhcbsimulate.cwl

  # Reconstruction step
  reconstruction:
    in:
      run-id: run-id
      task-id: task-id
      files: simulation/sim
    out:
      - results
      - others
    run: ./lhcbreconstruct.cwl

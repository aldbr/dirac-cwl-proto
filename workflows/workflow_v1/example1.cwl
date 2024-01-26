cwlVersion: v1.2
class: Workflow
label: "My Analysis Workflow"
doc: >
  This workflow processes data through several steps including
  simulation, data resolution, and processing. It's designed for
  XYZ analysis.

# Define the inputs for the workflow
inputs:
  simulation-config: File  # Parameters for the simulation step
  input_data: File[] # Input data files for the processing step

# Define the outputs of the workflow
outputs:
  processing_results:
    type: File[]
    outputSource: processing/results

# Requirements for the workflow
requirements:
  SubworkflowFeatureRequirement: {}

# Define the steps of the workflow
steps:
  # Simulation step
  simulation:
    in:
      simulation-config: simulation-config
    out: [input_data_query]

    # Nested workflow for the simulation step
    run:
      class: Workflow
      requirements:
        ResourceRequirement:
          coresMin: 1
          ramMin: 2048

      inputs:
        simulation-config: File

      outputs:
        input_data_query:
          type: File
          outputSource: resolve_input_data_query/input_data_query

      steps:
        # First part of the simulation: running the simulation script
        simulate:
          in:
            simulation_config: simulation-config
          out: [result_sim, input_data_query_parameters]
          run:
            class: CommandLineTool
            baseCommand: [python, ../src/dirac_cwl_proto/modules/simulate.py]
            inputs:
              simulation_config: File
            outputs:
              result_sim:
                type: File
              input_data_query_parameters:
                type: string[]

        # Second part of the simulation: resolving input data query
        resolve_input_data_query:
          in:
            input_data_query_parameters: simulate/input_data_query_parameters
          out: [input_data_query]
          run:
            class: CommandLineTool
            baseCommand: [python, ../src/dirac_cwl_proto/modules/resolve_input_data_query.py]
            inputs:
              input_data_query_parameters: string[]
            outputs:
              input_data_query:
                type: File

  # Processing step
  processing:
    in:
      input_data: input_data
    out: [results]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 4
          ramMin: 2048
      inputs:
        input_data: File[]
      outputs:
        results:
          type: File[]
          outputBinding:
            glob: "*"
      baseCommand: [python, ../src/dirac_cwl_proto/modules/process.py]

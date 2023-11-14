cwlVersion: v1.2
class: Workflow

requirements:
  ResourceRequirement:
    coresMin: 2
    ramMin: 2048

inputs:
  parameters: string[]  # Input parameters for the simulation step

steps:
  simulation:
    in:
      parameters: parameters
    run:
      class: Workflow
      steps:
        simulation_step:
          in:
            parameters: parameters
          run: sim.py
          out: [result_sim, input_data_query_parameters]

        resolve_input_data_query:
          in:
            input_data_query_parameters: simulation_step/input_data_query_parameters
          run: resolve_input_data_query.py
          out: [input_data_query]
    out: [input_data_query]

  processing:
    in:
      input: simulation/input_data_query
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 2
          ramMin: 2048
      inputs:
        files: File[]
      baseCommand: processing.py
      outputBinding:
        glob: "*"
    out: [results]

outputs:
  processing_results:
    type: File[]
    outputSource: processing/results


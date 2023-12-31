class: Workflow
cwlVersion: v1.2
inputs:
  simulation-config: File
outputs:
  input_data_query:
    outputSource: resolve_input_data_query/input_data_query
    type: File
requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 2048
steps:
  resolve_input_data_query:
    in:
      input_data_query_parameters: simulate/input_data_query_parameters
    out:
    - input_data_query
    run:
      baseCommand:
      - python
      - ../src/dirac_cwl_proto/modules/resolve_input_data_query.py
      class: CommandLineTool
      inputs:
        input_data_query_parameters: string[]
      outputs:
        input_data_query:
          type: File
  simulate:
    in:
      simulation_config: simulation-config
    out:
    - result_sim
    - input_data_query_parameters
    run:
      baseCommand:
      - python
      - ../src/dirac_cwl_proto/modules/simulate.py
      class: CommandLineTool
      inputs:
        simulation_config: File
      outputs:
        input_data_query_parameters:
          type: string[]
        result_sim:
          type: File

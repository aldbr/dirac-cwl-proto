cwlVersion: v1.2
class: Workflow

  requirements:
    ResourceRequirement:
      coresMin: 1
      ramMin: 2048

  inputs:
    parameters: string[]
      
  outputs:
    input_data_query:
    type: File
    outputSource: resolve_input_data_query/input_data_query
      
  steps:
    simulate:
      in:
        parameters: parameters
      out: [result_sim, input_data_query_parameters]

      run: python ../src/dirac_cwl_proto/modules/sim.py

    resolve_input_data_query:
      in:
        input_data_query_parameters: simulate/input_data_query_parameters
      out: [input_data_query]

      run: python ../src/dirac_cwl_proto/modules/resolve_input_data_query.py
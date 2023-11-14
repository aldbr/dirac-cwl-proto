cwlVersion: v1.2
class: Workflow

inputs:
  parameters: string[]
  input_data: File[]

outputs:
  processing_results:
    type: File[]
    outputSource: processing/results

requirements:
  SubworkflowFeatureRequirement: {}

steps:
  simulation:
    in:
      parameters: parameters
    out: [input_data_query]
    
    run:
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

          run:
            inputs:
              parameters: string[]
      
            outputs:
              result_sim:
                type: File
              input_data_query_parameters:
                type: string[]

            class: CommandLineTool
            baseCommand: [python, ../src/dirac_cwl_proto/modules/sim.py]

        resolve_input_data_query:
          in:
            input_data_query_parameters: simulate/input_data_query_parameters
          out: [input_data_query]

          run:
            inputs:
              input_data_query_parameters: string[]
      
            outputs:
              input_data_query:
                type: File

            class: CommandLineTool
            baseCommand: [python, ../src/dirac_cwl_proto/modules/resolve_input_data_query.py]

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

      baseCommand: [python, ../src/dirac_cwl_proto/modules/processing.py]
      
      

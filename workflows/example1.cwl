cwlVersion: v1.2
class: Workflow

inputs:
  parameters: string[]

outputs:
  processing_results:
    type: File[]
    outputSource: processing/results

steps:
  simulation:
    in:
      parameters: parameters
    out: [input_data_query]
    
    run: example1.simulation.cwl

  processing:
    in:
      input: simulation/input_data_query
    out: [results]

    run:
      class: CommandLineTool
      
      requirements:
        ResourceRequirement:
          coresMin: 4
          ramMin: 2048
      
      inputs:
        files: File[]
      
      outputs:
        results:
          type: File[]
          outputBinding:
            glob: "*"

      baseCommand: [python, ../src/dirac_cwl_proto/modules/processing.py]
      
      

cwlVersion: v1.2
class: Workflow
label: "Fit data Workflow"
doc: >
  This workflow is composed of 2 Workflow steps:
  - data-generation: produce data
  - fit: run the fit of two algorithms

# Define the inputs of the workflow
inputs:
  data_file_1:
    type: string
    default: data_1.txt
  data_file_2:
    type: string
    default: data_2.txt


# Define the outputs of the workflow
outputs:
  fit-data:
    type: File[]?
    outputSource:
      - data-generation/data
      - fit/fit-data
  # logs:
  #   type: File[]?
  #   outputSource:
  #     #- data-generation/log
  #     - fit/log
  #   linkMerge: merge_flattened


# Requirements for the workflow
requirements:
  SubworkflowFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}

# Define the steps of the workflow
steps:
  # Producing the data
  data-generation:
    in: 
      data_file_1: data_file_1
      data_file_2: data_file_2
    out: [data]
    run:
      class: Workflow
      requirements:
        ResourceRequirement:
          coresMin: 1
          coresMax: 4
          ramMin: 1024
          ramMax: 4096
      inputs:
        data_file_1:
          type: string
        data_file_2:
          type: string
      outputs:
        data:
          type: File[]?
          outputSource: 
            - gen-data-1/data
            - gen-data-2/data
        # log:
        #   type: File[]?
        #   outputSource: 
        #     - gen-data-1/log
        #     - gen-data-2/log

      steps:
        # Generate first set of data
        gen-data-1:
          in:
            data_file_1: data_file_1
          out: 
            - data
          run:
            class: CommandLineTool
            baseCommand: ["random-data-gen"]
            inputs:
              data_file_1:
                type: string
                inputBinding:
                  prefix: "--output_file"

            outputs:
              data:
                type: File[]?
                outputBinding:
                  glob: "data*.txt"
              # log:
              #   type: File[]?
              #   outputBinding:
              #     glob: ["gen.log"]

        # Generate second set of data
        gen-data-2:
          in:
            data_file_2: data_file_2
          out: 
            - data
          run:
            class: CommandLineTool
            baseCommand: ["random-data-gen"]

            inputs:
              data_file_2:
                type: string
                inputBinding:
                  prefix: "--output_file"

            outputs:
              data:
                type: File[]?
                outputBinding:
                  glob: "data*.txt"
              # log:
              #   type: File[]?
              #   outputBinding:
              #     glob: ["gen.log"]

  # Run fit
  fit:
    in:
      data: data-generation/data
    out:
      - fit-data
    run:
      class: Workflow
      requirements:
        ResourceRequirement:
          coresMin: 1
          coresMax: 4
          ramMin: 1024
          ramMax: 4096

      inputs:
        data:
          type: File[]?

      outputs:
        fit-data:
          type: File[]?
          outputSource: 
            - run-fit-1/fit-data
            - run-fit-2/fit-data
        # log:
        #   type: File[]?
        #   outputSource:
        #     - run-fit-1/log
        #     - run-fit-2/log

      steps:
        # Run fit 1
        run-fit-1:
          in: 
            data: data
          out: 
            - fit-data
          run:
            class: CommandLineTool
            baseCommand: ["gaussian-fit"]
            requirements:
              InitialWorkDirRequirement:
                listing:
                  - $(inputs.data)  # Stage the data files
            inputs:
              data:
                type: File[]?
        
            outputs:
              fit-data:
                type: File[]?
                outputBinding:
                  glob: ["fit*.txt"]
              # log:
              #   type: File[]?
              #   outputBinding:
              #     glob: ["*log"]

        # Run fit 2
        run-fit-2:
          in: 
            data: data
          out: 
            - fit-data
          run:
            class: CommandLineTool
            baseCommand: ["gaussian-fit"]
            requirements:
              InitialWorkDirRequirement:
                listing:
                  - $(inputs.data)  # Stage the data files

            inputs:
              data:
                type: File[]?

            outputs:
              fit-data:
                type: File[]?
                outputBinding:
                  glob: ["fit*.txt"]
              # log:
              #   type: File[]?
              #   outputBinding:
              #     glob: ["*log"]

cwlVersion: v1.2
class: Workflow
label: "Main Workflow"
doc: >
  This workflow is composed of two dependent workflows
  each composed of two command line tools:
  - data-generation: produce data
  - fit: run gaussian fit on data

requirements:
  SubworkflowFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}

inputs:
  output_file_name_1:
    type: string
    default: data_gen1.txt
  output_file_name_2:
    type: string
    default: data_gen2.txt

outputs:
  fit-data:
    type: File[]
    outputSource:
      - fit/fit-data
    linkMerge: merge_flattened
  logs:
    type: File[]?
    outputSource:
      - fit/log
    linkMerge: merge_flattened

steps:
  data-generation:
    run: ./data_generation/data-generation-workflow.cwl
    in:
      output_file_name_1: output_file_name_1
      output_file_name_2: output_file_name_2
    out: [data1, data2, log1, log2]

  fit:
    run: ./gaussian_fit/gaussian-fit-workflow.cwl
    in:
      data1: data-generation/data1
      data2: data-generation/data2
    out: [fit-data, log]

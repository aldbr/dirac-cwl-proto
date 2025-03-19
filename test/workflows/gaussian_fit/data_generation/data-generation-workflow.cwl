cwlVersion: v1.2
class: Workflow
label: "Data Generation Workflow"
doc: >
  This workflow generates data using two independent data-generation tools.

inputs:
  output_file_name_1:
    type: string
    default: "data1.txt"
  output_file_name_2:
    type: string
    default: "data2.txt"

outputs:
  data1:
    type: File[]
    outputSource: data-generation-1/data
  data2:
    type: File[]
    outputSource: data-generation-2/data
  log1:
    type: File[]
    outputSource: data-generation-1/log
  log2:
    type: File[]
    outputSource: data-generation-2/log

steps:
  data-generation-1:
    run: data-generation.cwl
    in:
      output_file_name: output_file_name_1
    out: [data, log]

  data-generation-2:
    run: data-generation.cwl
    in:
      output_file_name: output_file_name_2
    out: [data, log]

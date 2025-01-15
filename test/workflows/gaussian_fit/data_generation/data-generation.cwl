cwlVersion: v1.2
class: CommandLineTool
label: "Benchmark Data Generation Tool"

inputs:
  output_file_name:
    type: string
    default: data.txt
    inputBinding:
      prefix: "--file-path"

outputs:
  data:
    type: File
    outputBinding:
      glob: $(inputs.output_file_name)
  log:
    type: File[]?
    outputBinding:
      glob: "*.log"

baseCommand: ["random-data-gen"]
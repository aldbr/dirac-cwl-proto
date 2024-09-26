cwlVersion: v1.2
class: CommandLineTool
label: "Caesar Cipher"

requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 1024
baseCommand: ["crypto", "caesar"]

inputs:
  input_string:
    type: string
    inputBinding:
      position: 1
  shift_value:
    type: int
    inputBinding:
      position: 2

outputs:
  output:
    type: File
    outputBinding:
      glob: "caesar_result.txt"

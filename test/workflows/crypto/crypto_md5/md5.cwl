cwlVersion: v1.2
class: CommandLineTool
label: "MD5 Cipher"

requirements:
  ResourceRequirement:
    coresMin: 1
    ramMin: 1024
baseCommand: ["crypto", "md5"]

inputs:
  input_string:
    type: string
    default: "Hello, World!"
    inputBinding:
      position: 1

outputs:
  output:
    type: File
    outputBinding:
      glob: "md5_result.txt"

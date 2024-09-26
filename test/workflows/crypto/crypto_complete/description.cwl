cwlVersion: v1.2
class: Workflow
label: "CryptoFun Workflow"
doc: >
  This workflow demonstrates basic cryptographic transformations. It takes a string input and processes it through four independent CommandLineTool steps:
  a Caesar cipher, Base64 encoding, MD5 hashing, and ROT13 encryption.

inputs:
  input_string:
    type: string
    default: "Hello, World!"
    doc: "The input string to encrypt"
  shift_value:
    type: int
    default: 3
    doc: "The Caesar cipher shift value"

outputs:
  caesar_output:
    type: File
    outputSource: caesar_step/output
    doc: "Caesar cipher output"
  base64_output:
    type: File
    outputSource: base64_step/output
    doc: "Base64 encoded output"
  md5_output:
    type: File
    outputSource: md5_step/output
    doc: "MD5 hash output"
  rot13_output:
    type: File
    outputSource: rot13_step/output
    doc: "ROT13 encrypted output"

steps:
  caesar_step:
    in:
      input_string: input_string
      shift_value: shift_value
    out: [output]
    run:
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

  base64_step:
    in:
      input_string: input_string
    out: [output]
    run:
      class: CommandLineTool
      label: "Base64 Encoding"
      requirements:
        ResourceRequirement:
          coresMin: 1
          ramMin: 1024
      baseCommand: ["crypto", "base64"]
      inputs:
        input_string:
          type: string
          inputBinding:
            position: 1
      outputs:
        output:
          type: File
          outputBinding:
            glob: "base64_result.txt"

  md5_step:
    in:
      input_string: input_string
    out: [output]
    run:
      class: CommandLineTool
      label: "MD5 Hashing"
      requirements:
        ResourceRequirement:
          coresMin: 1
          ramMin: 1024
      baseCommand: ["crypto", "md5"]
      inputs:
        input_string:
          type: string
          inputBinding:
            position: 1
      outputs:
        output:
          type: File
          outputBinding:
            glob: "md5_result.txt"


  rot13_step:
    in:
      input_string: input_string
    out: [output]
    run:
      class: CommandLineTool
      label: "ROT13 Encryption"
      requirements:
        ResourceRequirement:
          coresMin: 1
          ramMin: 1024
      baseCommand: ["crypto", "rot13"]
      inputs:
        input_string:
          type: string
          inputBinding:
            position: 1
      outputs:
        output:
          type: File
          outputBinding:
            glob: "rot13_result.txt"

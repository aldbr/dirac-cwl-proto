cwlVersion: v1.2
# What type of CWL process we have in this document: ComandLineTool or Workflow.
class: CommandLineTool

# The inputs for this process: none.
inputs: []
# The outputs for this process: file1
outputs:
  output1:
    type: File
    outputBinding:
      glob: "file1*"

baseCommand: ["touch", "file1"]

$namespaces:
  dirac: "../test_meta/schemas/dirac-metadata.json#" # Generated schema from Pydantic models
hints:
  dirac:execution-hooks:
    hook_plugin: "QueryBasedPlugin"
    output_paths:
      output1: "lfn:test/output"

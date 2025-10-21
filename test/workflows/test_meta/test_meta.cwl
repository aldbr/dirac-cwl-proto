cwlVersion: v1.2
# What type of CWL process we have in this document: ComandLineTool or Workflow.
class: CommandLineTool

# The inputs for this process: none.
inputs: []
# The outputs for this process: none.
outputs: []

baseCommand: ["echo", "Hello World"]

$namespaces:
  dirac: "test/workflows/test_meta/schemas/dirac-metadata.json" # Generated schema from Pydantic models
hints:
  dirac:execution-hooks:
    hook_plugin: UserPlugin
    configuration:
      campaign: PROD5
      site: LaPalma
  dirac:scheduling:
    platform: x86_64
    priority: 10
    sites: null

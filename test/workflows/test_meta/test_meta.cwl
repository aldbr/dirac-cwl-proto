cwlVersion: v1.2
# What type of CWL process we have in this document: ComandLineTool or Workflow.
class: CommandLineTool

# The inputs for this process: none.
inputs: [ ]
# The outputs for this process: none.
outputs: [ ]

baseCommand: [ "echo", "Hello World" ]

$namespaces:
  dirac: "../../schemas/dirac-metadata.json#/$defs/" # Generated schema from Pydantic models

$schemas:
  - "../../schemas/dirac-metadata.json"

hints:
  - class: dirac:ExecutionHooksHint
    hook_plugin: "QueryBased" # should be overrided by inputs with cwl:override (how?)
    configuration:
      campaign: PROD5
      site: LaPalma

  - class: dirac:SchedulingHint
    platform: x86_64
    priority: 10
    sites: null

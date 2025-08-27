cwlVersion: v1.2
# What type of CWL process we have in this document: ComandLineTool or Workflow.
class: CommandLineTool

# The inputs for this process: none.
inputs: []
# The outputs for this process: none.
outputs: []

baseCommand: ["echo", "Hello World"]

$namespaces:
  dirac: "./schemas/dirac-metadata.json#" # Generated schema from Pydantic models
hints:
  dirac:metadata:
    metadata_class: "User"
    query_params:
      campaign: PROD5
      site: LaPalma
  dirac:description:
    platform: x86_64
    priority: 10
    sites: null

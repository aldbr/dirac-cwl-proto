cwlVersion: v1.2
class: CommandLineTool
label: "Hello World Command"
doc: >
  This is a very simple CommandLineTool that cannot be executed as a production.

inputs: []
outputs: []


requirements:
  ResourceRequirement:
    coresMin: 4
    ramMin: 2048

baseCommand: ["echo", "Hello World"]

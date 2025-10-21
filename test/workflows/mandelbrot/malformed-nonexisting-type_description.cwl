cwlVersion: v1.2
class: Workflow
label: "Mandelbrot Workflow"
doc: >
  This workflow is composed of 2 Workflow steps:
  - image-prod: produce an image
  - image-merge: merge the images produced by several image-prod

# Define the inputs of the workflow
inputs:
  precision:
    type: float
    default: 0.0005
  max_iterations:
    type: int
    default: 1000
  start_x:
    type: float
    default: -0.5
  start_y:
    type: float
    default: 0.0
  step:
    type: int
    default: 1
  split:
    type: int
    default: 1
  width:
    type: int
    default: 7680
  height:
    type: int
    default: 4320
  output_name:
    type: string
    default: "data"

# Define the outputs of the workflow
outputs:
  merged-data:
    type: File[]?
    outputSource: image-merge/data-merged
  logs:
    type: File[]?
    outputSource:
      - image-prod/log
      - image-merge/log
    linkMerge: merge_flattened

hints:
  $import: "./type_dependencies/production/malformed-nonexisting-type_metadata-mandelbrot_complete.yaml"

# Requirements for the workflow
requirements:
  SubworkflowFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}

# Define the steps of the workflow
steps:
  # Producing the image
  image-prod:
    in:
      precision: precision
      max_iterations: max_iterations
      start_x: start_x
      start_y: start_y
      step: step
      split: split
      width: width
      height: height
      output_name: output_name
    out:
      - data
      - log
    run: ./image-prod.cwl

  # Merge the images
  image-merge:
    in:
      data: image-prod/data
    out:
      - data-merged
      - log
    run: ./image-merge.cwl

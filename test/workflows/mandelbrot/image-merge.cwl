cwlVersion: v1.2
label: "Mandelbrot Workflow - Image Merge step"
class: Workflow

inputs:
  data:
    type: File[]?

outputs:
  data-merged:
    type: File[]?
    outputSource: merge-mandelbrot/data-merged
  log:
    type: File[]?
    outputSource: merge-mandelbrot/log

hints:
  $import: "type_dependencies/transformation/metadata-mandelbrot_imagemerge.yaml"

steps:
  # Clone the project
  get-mandelbrot:
    in: []
    out: [repo]
    run:
      class: CommandLineTool
      baseCommand: ["git", "clone", "https://gitlab.cta-observatory.org/arrabito/mandel4ts.git"]
      inputs: []
      outputs:
        repo:
          type: Directory
          outputBinding:
            glob: "mandel4ts"

  merge-mandelbrot:
    in:
      data: data
      repo: get-mandelbrot/repo
    out: [data-merged, log]
    run:
      class: CommandLineTool
      baseCommand: ["python", "./mandel4ts/src/mandel4ts/create_bitmap_image.py"]
      requirements:
        InitialWorkDirRequirement:
          listing:
            - $(inputs.repo)  # Stage the repo directory
            - $(inputs.data)  # Stage the data files

      inputs:
        data:
          type: File[]?
        repo:
          type: Directory

      outputs:
        data-merged:
          type: File[]?
          outputBinding:
            glob: ["mandelbrot_image*.bmp"]
        log:
          type: File[]?
          outputBinding:
            glob: ["*log"]

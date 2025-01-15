cwlVersion: v1.2
label: "Benchmark execution Workflow"
class: Workflow

inputs:
  data:
    type: File[]?

outputs:
  benchmark-data:
    type: File[]?
    outputSource: 
      - run-benchmark-1/benchmark-data
      - run-benchmark-2/benchmark-data
  log:
    type: File[]?
    outputSource:
      - run-benchmark-1/log
      - run-benchmark-2/log

steps:
  # Run benchmark 1
  run-benchmark-1:
    in: 
      data: data
    out: [benchmark-1-data, benchmark-1-log]
    run:
      class: CommandLineTool
      baseCommand: ["python", "run_benchmark_1.py"]
      inputs:
        data:
          type: File[]?
        repo:
          type: Directory
  
      outputs:
        benchmark-data:
          type: File[]?
          outputBinding:
            glob: ["benchmark.txt"]
        log:
          type: File[]?
          outputBinding:
            glob: ["*log"]

  # Run benchmark 2
  run-benchmark-2:
    in:
      data: data
    out: [benchmark-2-data, benchmark-2-log]
    run:
      class: CommandLineTool
      baseCommand: ["python", "benchmark_2.py"]
      requirements:
        InitialWorkDirRequirement:
          listing:
            - $(inputs.data)  # Stage the data files

      inputs:
        data:
          type: File[]?
        repo:
          type: Directory

      outputs:
        benchmark-data:
          type: File[]?
          outputBinding:
            glob: ["benchmark.txt"]
        log:
          type: File[]?
          outputBinding:
            glob: ["*log"]

cwlVersion: v1.2
class: Workflow
label: "Benchmark Workflow"
doc: >
  This workflow is composed of 2 Workflow steps:
  - data-generation: produce data
  - benchmark: run the benchmark of two algorithms

# Define the inputs of the workflow
inputs:
  random_seed:
    type: float
    default: 0.0005


# Define the outputs of the workflow
outputs:
  benchmark-data:
    type: File[]?
    outputSource:
      - data-generation/data
      - benchmark/benchmark-data
  logs:
    type: File[]?
    outputSource:
      - data-generation/log
      - benchmark/log
    linkMerge: merge_flattened


# Requirements for the workflow
requirements:
  SubworkflowFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}

# Define the steps of the workflow
steps:
  # Producing the data
  data-generation:
    in: 
      random_seed: random_seed
    out: [data, log]
    run:
      class: Workflow
      requirements:
        ResourceRequirement:
          coresMin: 1
          coresMax: 4
          ramMin: 1024
          ramMax: 4096

      inputs:
        random_seed:
          type: float
      outputs:
        data:
          type: File[]?
          outputSource: 
            - gen-data-1/data
            - gen-data-2/data
        log:
          type: File[]?
          outputSource: 
            - gen-data-1/log
            - gen-data-2/log

      steps:
        # Generate first set of data
        gen-data-1:
          in:
            random_seed: random_seed
          out: [data, log]
          run:
            class: CommandLineTool
            baseCommand: ["python", "generate_data.py"]
            inputs:
              random_seed:
                type: float
                inputBinding:
                  prefix: "--random_seed"

            outputs:
              data:
                type: File[]?
                outputBinding:
                  glob: "data.txt"
              log:
                type: File[]?
                outputBinding:
                  glob: ["gen.log"]

        # Generate second set of data
        gen-data-2:
          in:
            random_seed: random_seed
          out: [data, log]
          run:
            class: CommandLineTool
            baseCommand: ["python", "generate_data.py"]
            requirements:
              InitialWorkDirRequirement: []

            inputs:
              random_seed:
                type: float
                inputBinding:
                  prefix: "--random_seed"

            outputs:
              data:
                type: File[]?
                outputBinding:
                  glob: "data.txt"
              log:
                type: File[]?
                outputBinding:
                  glob: ["gen.log"]

  # Run benchmark
  benchmark:
    in:
      data: data-generation/data
    out:
      - benchmark-data
      - benchmark-log
    run:
      class: Workflow
      requirements:
        ResourceRequirement:
          coresMin: 1
          coresMax: 4
          ramMin: 1024
          ramMax: 4096

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

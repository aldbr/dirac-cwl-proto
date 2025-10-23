cwlVersion: v1.2
class: Workflow
requirements:
  MultipleInputFeatureRequirement: {}
  ResourceRequirement:
    coresMin: 1
    coresMax: 3
    ramMin: 2048
    ramMax: 4096

hints:
  $import: "type_dependencies/transformation/metadata-lhcb_reconstruct.yaml"

inputs:
  run-id:
    type: int
    default: 123
  task-id:
    type: int
    default: 456
  files:
    type: File[]?

outputs:
  results:
    type: File[]?
    outputSource:
      - digitization/digi
      - init_reconstruction_1/digi
      - init_reconstruction_2/digi
      - init_reconstruction_3/digi
      - full_event_reconstruction/dst
      - analysis_1/dst
      - analysis_2/dst
    linkMerge: merge_flattened
  others:
    type: File[]?
    outputSource:
      - digitization/others
      - init_reconstruction_1/others
      - init_reconstruction_2/others
      - init_reconstruction_3/others
      - full_event_reconstruction/others
      - analysis_1/others
      - analysis_2/others
    linkMerge: merge_flattened
steps:
  # Boole step
  digitization:
    in:
      run-id: run-id
      task-id: task-id
      files: files
    out: [digi, others]
    run:
      class: CommandLineTool
      baseCommand: [lhcb-app]

      requirements:
        InitialWorkDirRequirement:
          listing:
            - entryname: configuration.json
              entry: |
                {
                  "step_id": 1,
                  "application": {
                    "name": "Boole",
                    "version": "v30r4",
                    "extra_packages": ["AppConfig.v3r425"]
                  },
                  "input": {
                    "tck": ""
                  },
                  "output": {
                    "types": ["digi"]
                  },
                  "options": {
                    "options": [
                      "$APPCONFIGOPTS/Boole/Default.py",
                      "$APPCONFIGOPTS/Boole/EnableSpillover.py",
                      "$APPCONFIGOPTS/Boole/DataType-2015.py",
                      "$APPCONFIGOPTS/Boole/Boole-SetOdinRndTrigger.py",
                      "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                    ]
                  },
                  "db_tags": {
                    "dddb_tag": "dddb-20170721-3",
                    "conddb_tag": "sim-20170721-2-vc-mu100"
                  }
                }

      inputs:
        files:
          type: File[]?
          inputBinding:
            prefix: "--files"
            itemSeparator: ","
        pool-xml-catalog:
          type: File?
          inputBinding:
            prefix: "--pool-xml-catalog"
        run-id:
          type: int
          inputBinding:
            prefix: "--run-id"
        task-id:
          type: int
          inputBinding:
            prefix: "--task-id"

      arguments:
        - position: 1
          valueFrom: "configuration.json"

      outputs:
        digi:
          type: File[]?
          outputBinding:
            glob: "*.digi"
        others:
          type: File[]?
          outputBinding:
            glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "prmon.log", "Boole*.log"]

  # Moore step 1
  init_reconstruction_1:
    in:
      run-id: run-id
      task-id: task-id
      files: [digitization/digi]
    out: [digi, others]
    run:
      class: CommandLineTool
      baseCommand: [lhcb-app]

      requirements:
        InitialWorkDirRequirement:
          listing:
            - entryname: configuration.json
              entry: |
                {
                  "step_id": 2,
                  "application": {
                    "name": "Moore",
                    "version": "v25r5p3",
                    "extra_packages": ["AppConfig.v3r425"]
                  },
                  "input": {
                    "tck": ""
                  },
                  "output": {
                    "types": ["digi"]
                  },
                  "options": {
                    "options": [
                      "$APPCONFIGOPTS/L0App/L0AppSimProduction.py",
                      "$APPCONFIGOPTS/L0App/L0AppTCK-0x160F.py",
                      "$APPCONFIGOPTS/L0App/ForceLUTVersionV8.py",
                      "$APPCONFIGOPTS/L0App/DataType-2016.py",
                      "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                    ],
                    "format": "l0app"
                  },
                  "db_tags": {
                    "dddb_tag": "dddb-20170721-3",
                    "conddb_tag": "sim-20170721-2-vc-mu100"
                  }
                }

      inputs:
        files:
          type: File[]?
          inputBinding:
            prefix: "--files"
            itemSeparator: ","
        pool-xml-catalog:
          type: File?
          inputBinding:
            prefix: "--pool-xml-catalog"
        run-id:
          type: int
          inputBinding:
            prefix: "--run-id"
        task-id:
          type: int
          inputBinding:
            prefix: "--task-id"

      arguments:
        - position: 1
          valueFrom: "configuration.json"

      outputs:
        digi:
          type: File[]?
          outputBinding:
            glob: "*.digi"
        others:
          type: File[]?
          outputBinding:
            glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "prmon.log", "Moore*.log"]

  # Moore step 2
  init_reconstruction_2:
    in:
      run-id: run-id
      task-id: task-id
      files: [init_reconstruction_1/digi]
    out: [digi, others]
    run:
      class: CommandLineTool
      baseCommand: [lhcb-app]

      requirements:
        InitialWorkDirRequirement:
          listing:
            - entryname: configuration.json
              entry: |
                {
                  "step_id": 3,
                  "application": {
                    "name": "Moore",
                    "version": "v25r5p3",
                    "extra_packages": ["AppConfig.v3r425"]
                  },
                  "input": {
                    "tck": ""
                  },
                  "output": {
                    "types": ["digi"]
                  },
                  "options": {
                    "options": [
                      "$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep2015.py",
                      "$APPCONFIGOPTS/Conditions/TCK-0x5138160F.py",
                      "$APPCONFIGOPTS/Moore/DataType-2016.py",
                      "$APPCONFIGOPTS/Moore/MooreSimProductionHlt1.py",
                      "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                    ]
                  },
                  "db_tags": {
                    "dddb_tag": "dddb-20170721-3",
                    "conddb_tag": "sim-20170721-2-vc-mu100"
                  }
                }

      inputs:
        files:
          type: File[]?
          inputBinding:
            prefix: "--files"
            itemSeparator: ","
        pool-xml-catalog:
          type: File?
          inputBinding:
            prefix: "--pool-xml-catalog"
        run-id:
          type: int
          inputBinding:
            prefix: "--run-id"
        task-id:
          type: int
          inputBinding:
            prefix: "--task-id"

      arguments:
        - position: 1
          valueFrom: "configuration.json"

      outputs:
        digi:
          type: File[]?
          outputBinding:
            glob: "*.digi"
        others:
          type: File[]?
          outputBinding:
            glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "prmon.log", "Moore*.log"]

  # Moore step 3
  init_reconstruction_3:
    in:
      run-id: run-id
      task-id: task-id
      files: [init_reconstruction_2/digi]
    out: [digi, others]
    run:
      class: CommandLineTool
      baseCommand: [lhcb-app]

      requirements:
        InitialWorkDirRequirement:
          listing:
            - entryname: configuration.json
              entry: |
                {
                  "step_id": 4,
                  "application": {
                    "name": "Moore",
                    "version": "v25r5p3",
                    "extra_packages": ["AppConfig.v3r425"]
                  },
                  "input": {
                    "tck": ""
                  },
                  "output": {
                    "types": ["digi"]
                  },
                  "options": {
                    "options": [
                      "$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep2015.py",
                      "$APPCONFIGOPTS/Conditions/TCK-0x6139160F.py",
                      "$APPCONFIGOPTS/Moore/DataType-2016.py",
                      "$APPCONFIGOPTS/Moore/MooreSimProductionHlt2.py",
                      "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                    ]
                  },
                  "db_tags": {
                    "dddb_tag": "dddb-20170721-3",
                    "conddb_tag": "sim-20170721-2-vc-mu100"
                  }
                }

      inputs:
        files:
          type: File[]?
          inputBinding:
            prefix: "--files"
            itemSeparator: ","
        pool-xml-catalog:
          type: File?
          inputBinding:
            prefix: "--pool-xml-catalog"
        run-id:
          type: int
          inputBinding:
            prefix: "--run-id"
        task-id:
          type: int
          inputBinding:
            prefix: "--task-id"

      arguments:
        - position: 1
          valueFrom: "configuration.json"

      outputs:
        digi:
          type: File[]?
          outputBinding:
            glob: "*.digi"
        others:
          type: File[]?
          outputBinding:
            glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "prmon.log", "Moore*.log"]

  # Brunel
  full_event_reconstruction:
    in:
      run-id: run-id
      task-id: task-id
      files: [init_reconstruction_3/digi]
    out: [dst, others]
    run:
      class: CommandLineTool
      baseCommand: [lhcb-app]

      requirements:
        InitialWorkDirRequirement:
          listing:
            - entryname: configuration.json
              entry: |
                {
                  "step_id": 5,
                  "application": {
                    "name": "Brunel",
                    "version": "v50r7",
                    "extra_packages": ["AppConfig.v3r425"]
                  },
                  "input": {
                    "tck": ""
                  },
                  "output": {
                    "types": ["dst"]
                  },
                  "options": {
                    "options": [
                      "$APPCONFIGOPTS/Brunel/DataType-2016.py",
                      "$APPCONFIGOPTS/Brunel/MC-WithTruth.py",
                      "$APPCONFIGOPTS/Brunel/SplitRawEventOutput.4.3.py",
                      "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                    ]
                  },
                  "db_tags": {
                    "dddb_tag": "dddb-20170721-3",
                    "conddb_tag": "sim-20170721-2-vc-mu100"
                  }
                }


      inputs:
        files:
          type: File[]?
          inputBinding:
            prefix: "--files"
            itemSeparator: ","
        pool-xml-catalog:
          type: File?
          inputBinding:
            prefix: "--pool-xml-catalog"
        run-id:
          type: int
          inputBinding:
            prefix: "--run-id"
        task-id:
          type: int
          inputBinding:
            prefix: "--task-id"

      arguments:
        - position: 1
          valueFrom: "configuration.json"

      outputs:
        dst:
          type: File[]?
          outputBinding:
            glob: "*.dst"
        others:
          type: File[]?
          outputBinding:
            glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "prmon.log", "Brunel*.log"]

  # DaVinci step 1
  analysis_1:
    in:
      run-id: run-id
      task-id: task-id
      files: [full_event_reconstruction/dst]
      secondary-files: [init_reconstruction_3/digi]
    out: [dst, others]
    run:
      class: CommandLineTool
      baseCommand: [lhcb-app]

      requirements:
        InitialWorkDirRequirement:
          listing:
            - entryname: configuration.json
              entry: |
                {
                  "step_id": 6,
                  "application": {
                    "name": "DaVinci",
                    "version": "v41r5",
                    "extra_packages": ["AppConfig.v3r425","TurboStreamProd.v4r2p9"]
                  },
                  "input": {
                    "tck": ""
                  },
                  "output": {
                    "types": ["dst"]
                  },
                  "options": {
                    "options": [
                      "$APPCONFIGOPTS/Turbo/Tesla_2016_LinesFromStreams_MC.py",
                      "$APPCONFIGOPTS/Turbo/Tesla_PR_Truth_2016.py",
                      "$APPCONFIGOPTS/Turbo/Tesla_Simulation_2016.py",
                      "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                    ],
                    "format": "Tesla"
                  },
                  "db_tags": {
                    "dddb_tag": "dddb-20170721-3",
                    "conddb_tag": "sim-20170721-2-vc-mu100"
                  }
                }
      inputs:
        files:
          type: File[]?
          inputBinding:
            prefix: "--files"
            itemSeparator: ","
        pool-xml-catalog:
          type: File?
          inputBinding:
            prefix: "--pool-xml-catalog"
        secondary-files:
          type: File[]?
          inputBinding:
            prefix: "--secondary-files"
        run-id:
          type: int
          inputBinding:
            prefix: "--run-id"
        task-id:
          type: int
          inputBinding:
            prefix: "--task-id"

      arguments:
        - position: 1
          valueFrom: "configuration.json"

      outputs:
        dst:
          type: File[]?
          outputBinding:
            glob: "*.dst"
        others:
          type: File[]?
          outputBinding:
            glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "prmon.log", "DaVinci*.log"]

  # DaVinci step 2
  analysis_2:
    in:
      run-id: run-id
      task-id: task-id
      files: [analysis_1/dst]
    out: [dst, others]
    run:
      class: CommandLineTool
      baseCommand: [lhcb-app]

      requirements:
        InitialWorkDirRequirement:
          listing:
            - entryname: configuration.json
              entry: |
                {
                  "step_id": 7,
                  "application": {
                    "name": "DaVinci",
                    "version": "v44r11p6",
                    "extra_packages": ["AppConfig.v3r425","WG/RDConfig.v1r119"]
                  },
                  "input": {
                    "tck": ""
                  },
                  "output": {
                    "types": ["tau2muphi.strip.dst"]
                  },
                  "options": {
                    "options": [
                      "$RDCONFIGOPTS/FilterTau2MuPhi-Stripping28r2p2.py",
                      "$APPCONFIGOPTS/DaVinci/DV-RedoCaloPID-Stripping_28_24.py",
                      "$APPCONFIGOPTS/DaVinci/DataType-2016.py",
                      "$APPCONFIGOPTS/DaVinci/InputType-DST.py",
                      "$APPCONFIGOPTS/DaVinci/DV-RawEventJuggler-4_3-to-4_3.py",
                      "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                    ]
                  },
                  "db_tags": {
                    "dddb_tag": "dddb-20170721-3",
                    "conddb_tag": "sim-20170721-2-vc-mu100"
                  }
                }

      inputs:
        files:
          type: File[]?
          inputBinding:
            prefix: "--files"
            itemSeparator: ","
        pool-xml-catalog:
          type: File?
          inputBinding:
            prefix: "--pool-xml-catalog"
        run-id:
          type: int
          inputBinding:
            prefix: "--run-id"
        task-id:
          type: int
          inputBinding:
            prefix: "--task-id"

      arguments:
        - position: 1
          valueFrom: "configuration.json"

      outputs:
        dst:
          type: File[]?
          outputBinding:
            glob: "*.Tau2MuPhi.Strip.dst"
        others:
          type: File[]?
          outputBinding:
            glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "prmon.log", "DaVinci*.log"]

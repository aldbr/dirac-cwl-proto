class: Workflow
inputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/files
  type:
  - 'null'
  - name: _:9485339e-5f07-4d98-adb1-2c00dfe0ece9
    items: File
    type: array
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/pool-xml-catalog
  type:
  - 'null'
  - File
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/run-id
  type: int
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/task-id
  type: int
outputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/others
  outputSource:
  - reconstruction/run/digitization/others
  - reconstruction/run/init_reconstruction_1/others
  - reconstruction/run/init_reconstruction_2/others
  - reconstruction/run/init_reconstruction_3/others
  - reconstruction/run/full_event_reconstruction/others
  - reconstruction/run/analysis_1/others
  - reconstruction/run/analysis_2/others
  linkMerge: merge_flattened
  type:
  - 'null'
  - name: _:dbb52020-2f1f-4986-9ccd-2b8bf33bc992
    items: File
    type: array
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/results
  outputSource:
  - reconstruction/run/digitization/digi
  - reconstruction/run/init_reconstruction_1/digi
  - reconstruction/run/init_reconstruction_2/digi
  - reconstruction/run/init_reconstruction_3/digi
  - reconstruction/run/full_event_reconstruction/dst
  - reconstruction/run/analysis_1/dst
  - reconstruction/run/analysis_2/dst
  linkMerge: merge_flattened
  type:
  - 'null'
  - name: _:6e809ab8-8d3d-4a1b-a6f0-26209984eaa8
    items: File
    type: array
requirements:
- class: MultipleInputFeatureRequirement
- class: ResourceRequirement
  coresMin: 1
  coresMax: 3
  ramMin: 2048
  ramMax: 4096
cwlVersion: v1.2
steps:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_1
  in:
  - id: files
    source:
    - reconstruction/run/full_event_reconstruction/dst
  - id: pool-xml-catalog
    source: reconstruction/run/pool-xml-catalog
  - id: run-id
    source: reconstruction/run/run-id
  - id: secondary-files
    source:
    - reconstruction/run/init_reconstruction_3/digi
  - id: task-id
    source: reconstruction/run/task-id
  out:
  - dst
  - others
  run:
    class: CommandLineTool
    id: _:a8bbf638-beeb-4421-98ea-ce0a72b703d0
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_1/run/files
      type:
      - 'null'
      - name: _:f892d25d-c07b-433c-91c0-ad956d8f89cf
        items: File
        type: array
      inputBinding:
        prefix: "--files"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_1/run/pool-xml-catalog
      type:
      - 'null'
      - File
      inputBinding:
        prefix: "--pool-xml-catalog"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_1/run/run-id
      type: int
      inputBinding:
        prefix: "--run-id"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_1/run/secondary-files
      type:
      - 'null'
      - name: _:a6f237f4-2cdb-4e91-b666-5a9aace0824b
        items: File
        type: array
      inputBinding:
        prefix: "--secondary-files"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_1/run/task-id
      type: int
      inputBinding:
        prefix: "--task-id"
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_1/run/dst
      type:
      - 'null'
      - name: _:f7196feb-3c2a-4e8a-b1b3-dd7dcde7862f
        items: File
        type: array
      outputBinding:
        glob: "*.dst"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_1/run/others
      type:
      - 'null'
      - name: _:361f5ef9-24bb-4d4e-a262-f685dab99165
        items: File
        type: array
      outputBinding:
        glob:
        - "prodConf*.json"
        - "prodConf*.py"
        - "summary*.xml"
        - "prmon.log"
        - "DaVinci*.log"
    requirements:
    - class: InitialWorkDirRequirement
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
    baseCommand:
    - lhcb_app.py
    arguments:
    - position: 1
      valueFrom: "configuration.json"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_2
  in:
  - id: files
    source:
    - reconstruction/run/analysis_1/dst
  - id: pool-xml-catalog
    source: reconstruction/run/pool-xml-catalog
  - id: run-id
    source: reconstruction/run/run-id
  - id: task-id
    source: reconstruction/run/task-id
  out:
  - dst
  - others
  run:
    class: CommandLineTool
    id: _:401d8b53-8ab8-4dc9-bf2e-ae9be5afdbf9
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_2/run/files
      type:
      - 'null'
      - name: _:52a7d233-1ffe-4089-b044-011198d54ad6
        items: File
        type: array
      inputBinding:
        prefix: "--files"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_2/run/pool-xml-catalog
      type:
      - 'null'
      - File
      inputBinding:
        prefix: "--pool-xml-catalog"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_2/run/run-id
      type: int
      inputBinding:
        prefix: "--run-id"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_2/run/task-id
      type: int
      inputBinding:
        prefix: "--task-id"
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_2/run/dst
      type:
      - 'null'
      - name: _:5b26f9c3-423f-456d-b15d-434d518d3bef
        items: File
        type: array
      outputBinding:
        glob: "*.Tau2MuPhi.Strip.dst"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/analysis_2/run/others
      type:
      - 'null'
      - name: _:55af8a68-5e1b-4fbc-b569-99fc555fd9c0
        items: File
        type: array
      outputBinding:
        glob:
        - "prodConf*.json"
        - "prodConf*.py"
        - "summary*.xml"
        - "prmon.log"
        - "DaVinci*.log"
    requirements:
    - class: InitialWorkDirRequirement
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
    baseCommand:
    - lhcb_app.py
    arguments:
    - position: 1
      valueFrom: "configuration.json"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/digitization
  in:
  - id: files
    source: reconstruction/run/files
  - id: pool-xml-catalog
    source: reconstruction/run/pool-xml-catalog
  - id: run-id
    source: reconstruction/run/run-id
  - id: task-id
    source: reconstruction/run/task-id
  out:
  - digi
  - others
  run:
    class: CommandLineTool
    id: _:81a5f532-481e-434e-a827-71f2caa40410
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/digitization/run/files
      type:
      - 'null'
      - name: _:642d4be2-444c-4c63-bd8d-9e47ac0b22f8
        items: File
        type: array
      inputBinding:
        prefix: "--files"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/digitization/run/pool-xml-catalog
      type:
      - 'null'
      - File
      inputBinding:
        prefix: "--pool-xml-catalog"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/digitization/run/run-id
      type: int
      inputBinding:
        prefix: "--run-id"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/digitization/run/task-id
      type: int
      inputBinding:
        prefix: "--task-id"
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/digitization/run/digi
      type:
      - 'null'
      - name: _:df2e491b-cba2-4264-ab44-8660d9f55ab5
        items: File
        type: array
      outputBinding:
        glob: "*.digi"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/digitization/run/others
      type:
      - 'null'
      - name: _:fc7b4c83-3ccb-4955-934e-9d0d7ab4b6ba
        items: File
        type: array
      outputBinding:
        glob:
        - "prodConf*.json"
        - "prodConf*.py"
        - "summary*.xml"
        - "prmon.log"
        - "Boole*.log"
    requirements:
    - class: InitialWorkDirRequirement
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
    baseCommand:
    - lhcb_app.py
    arguments:
    - position: 1
      valueFrom: "configuration.json"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/full_event_reconstruction
  in:
  - id: files
    source:
    - reconstruction/run/init_reconstruction_3/digi
  - id: pool-xml-catalog
    source: reconstruction/run/pool-xml-catalog
  - id: run-id
    source: reconstruction/run/run-id
  - id: task-id
    source: reconstruction/run/task-id
  out:
  - dst
  - others
  run:
    class: CommandLineTool
    id: _:47e79271-1add-4ea4-b015-4b95f9cd1184
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/full_event_reconstruction/run/files
      type:
      - 'null'
      - name: _:b04bc674-a12e-4398-8fc4-2d9faf27ec2a
        items: File
        type: array
      inputBinding:
        prefix: "--files"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/full_event_reconstruction/run/pool-xml-catalog
      type:
      - 'null'
      - File
      inputBinding:
        prefix: "--pool-xml-catalog"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/full_event_reconstruction/run/run-id
      type: int
      inputBinding:
        prefix: "--run-id"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/full_event_reconstruction/run/task-id
      type: int
      inputBinding:
        prefix: "--task-id"
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/full_event_reconstruction/run/dst
      type:
      - 'null'
      - name: _:db72f8dd-c29d-4f27-8787-6ca477afcd7b
        items: File
        type: array
      outputBinding:
        glob: "*.dst"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/full_event_reconstruction/run/others
      type:
      - 'null'
      - name: _:82b5ffda-35ff-41e6-9f17-cbcbe55f1cdc
        items: File
        type: array
      outputBinding:
        glob:
        - "prodConf*.json"
        - "prodConf*.py"
        - "summary*.xml"
        - "prmon.log"
        - "Brunel*.log"
    requirements:
    - class: InitialWorkDirRequirement
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
    baseCommand:
    - lhcb_app.py
    arguments:
    - position: 1
      valueFrom: "configuration.json"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_1
  in:
  - id: files
    source:
    - reconstruction/run/digitization/digi
  - id: pool-xml-catalog
    source: reconstruction/run/pool-xml-catalog
  - id: run-id
    source: reconstruction/run/run-id
  - id: task-id
    source: reconstruction/run/task-id
  out:
  - digi
  - others
  run:
    class: CommandLineTool
    id: _:5fae2ac8-feda-4ca3-8902-88da7882f8be
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_1/run/files
      type:
      - 'null'
      - name: _:b82625e4-2c9a-4e34-9668-e219e6a34f51
        items: File
        type: array
      inputBinding:
        prefix: "--files"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_1/run/pool-xml-catalog
      type:
      - 'null'
      - File
      inputBinding:
        prefix: "--pool-xml-catalog"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_1/run/run-id
      type: int
      inputBinding:
        prefix: "--run-id"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_1/run/task-id
      type: int
      inputBinding:
        prefix: "--task-id"
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_1/run/digi
      type:
      - 'null'
      - name: _:22264fce-4ba9-44ed-bd11-c9dc609bdabf
        items: File
        type: array
      outputBinding:
        glob: "*.digi"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_1/run/others
      type:
      - 'null'
      - name: _:96b940bf-cb92-4cfd-910e-cd13c9d13a80
        items: File
        type: array
      outputBinding:
        glob:
        - "prodConf*.json"
        - "prodConf*.py"
        - "summary*.xml"
        - "prmon.log"
        - "Moore*.log"
    requirements:
    - class: InitialWorkDirRequirement
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
    baseCommand:
    - lhcb_app.py
    arguments:
    - position: 1
      valueFrom: "configuration.json"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_2
  in:
  - id: files
    source:
    - reconstruction/run/init_reconstruction_1/digi
  - id: pool-xml-catalog
    source: reconstruction/run/pool-xml-catalog
  - id: run-id
    source: reconstruction/run/run-id
  - id: task-id
    source: reconstruction/run/task-id
  out:
  - digi
  - others
  run:
    class: CommandLineTool
    id: _:b562f38a-fb1b-40e3-9874-5649a75af297
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_2/run/files
      type:
      - 'null'
      - name: _:910a5c54-cd11-4ec0-b60a-b848326f2e72
        items: File
        type: array
      inputBinding:
        prefix: "--files"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_2/run/pool-xml-catalog
      type:
      - 'null'
      - File
      inputBinding:
        prefix: "--pool-xml-catalog"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_2/run/run-id
      type: int
      inputBinding:
        prefix: "--run-id"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_2/run/task-id
      type: int
      inputBinding:
        prefix: "--task-id"
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_2/run/digi
      type:
      - 'null'
      - name: _:1ab9dac4-323e-4aae-980f-a655e33f3e34
        items: File
        type: array
      outputBinding:
        glob: "*.digi"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_2/run/others
      type:
      - 'null'
      - name: _:d93abcec-c378-4b4b-8825-60b5c2ccc388
        items: File
        type: array
      outputBinding:
        glob:
        - "prodConf*.json"
        - "prodConf*.py"
        - "summary*.xml"
        - "prmon.log"
        - "Moore*.log"
    requirements:
    - class: InitialWorkDirRequirement
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
    baseCommand:
    - lhcb_app.py
    arguments:
    - position: 1
      valueFrom: "configuration.json"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_3
  in:
  - id: files
    source:
    - reconstruction/run/init_reconstruction_2/digi
  - id: pool-xml-catalog
    source: reconstruction/run/pool-xml-catalog
  - id: run-id
    source: reconstruction/run/run-id
  - id: task-id
    source: reconstruction/run/task-id
  out:
  - digi
  - others
  run:
    class: CommandLineTool
    id: _:9a7d4a20-de77-4abe-801c-5cde486f3a9e
    inputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_3/run/files
      type:
      - 'null'
      - name: _:3f47127f-a634-4a7d-83a7-0e31f3d91e8a
        items: File
        type: array
      inputBinding:
        prefix: "--files"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_3/run/pool-xml-catalog
      type:
      - 'null'
      - File
      inputBinding:
        prefix: "--pool-xml-catalog"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_3/run/run-id
      type: int
      inputBinding:
        prefix: "--run-id"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_3/run/task-id
      type: int
      inputBinding:
        prefix: "--task-id"
    outputs:
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_3/run/digi
      type:
      - 'null'
      - name: _:d7ae2579-c3ed-4e47-8e03-b14fe63ee032
        items: File
        type: array
      outputBinding:
        glob: "*.digi"
    - id:
        file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#reconstruction/run/init_reconstruction_3/run/others
      type:
      - 'null'
      - name: _:047ad528-58c7-4a0a-b9a5-a356bf9715a0
        items: File
        type: array
      outputBinding:
        glob:
        - "prodConf*.json"
        - "prodConf*.py"
        - "summary*.xml"
        - "prmon.log"
        - "Moore*.log"
    requirements:
    - class: InitialWorkDirRequirement
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
    baseCommand:
    - lhcb_app.py
    arguments:
    - position: 1
      valueFrom: "configuration.json"

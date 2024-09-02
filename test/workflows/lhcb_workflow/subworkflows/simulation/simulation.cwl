class: CommandLineTool
inputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#simulation/run/run-id
  type: int
  inputBinding:
    prefix: "--run-id"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#simulation/run/task-id
  type: int
  inputBinding:
    prefix: "--task-id"
outputs:
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#simulation/run/others
  type:
  - 'null'
  - name: _:b5ff098b-99c0-4ee5-90cf-02e33b456cc0
    items: File
    type: array
  outputBinding:
    glob:
    - "prodConf*.json"
    - "prodConf*.py"
    - "summary*.xml"
    - "GeneratorLog.xml"
    - "prmon.log"
    - "Gauss*.log"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#simulation/run/pool_xml_catalog
  type:
  - 'null'
  - File
  outputBinding:
    glob: "pool_xml_catalog.xml"
- id:
    file:///home/aldbr/Documents/CERN/engineering/projects/dirac-cwl-proto/workflows/lhcb_workflow/description.cwl#simulation/run/sim
  type:
  - 'null'
  - name: _:6b95555e-784c-42bc-9bf3-0b351f127e92
    items: File
    type: array
  outputBinding:
    glob: "*.sim"
requirements:
- class: InitialWorkDirRequirement
  listing:
  - entryname: configuration.json
    entry: |
      {
        "step_id": 0,
        "application": {
          "name": "Gauss",
          "version": "v49r25",
          "extra_packages": ["AppConfig.v3r425","Gen/DecFiles.v30r100"],
          "system_config": "x86_64-slc6-gcc48-opt"
        },
        "input": {
          "tck": "",
          "number_of_events": 2
        },
        "output": {
          "types": ["sim"],
          "histogram": true
        },
        "options": {
          "options": [
            "$APPCONFIGOPTS/Gauss/Beam6500GeV-mu100-2016-nu1.6.py",
            "$APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py",
            "$APPCONFIGOPTS/Gauss/DataType-2016.py",
            "$APPCONFIGOPTS/Gauss/RICHRandomHits.py",
            "$DECFILESROOT/options/23103004.py",
            "$LBPYTHIA8ROOT/options/Pythia8.py",
            "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py",
            "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
          ]
        },
        "db_tags": {
          "dddb_tag": "dddb-20170721-3",
          "conddb_tag": "sim-20170721-2-vc-mu100"
        }
      }
- class: ResourceRequirement
  coresMin: 4
  ramMin: 2048
cwlVersion: v1.2
baseCommand:
- lhcb_app.py
arguments:
- position: 1
  valueFrom: "configuration.json"
- prefix: "--pool-xml-catalog"
  valueFrom: "pool_xml_catalog.xml"

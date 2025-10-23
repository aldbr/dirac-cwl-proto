cwlVersion: v1.2
class: CommandLineTool
baseCommand: [lhcb-app]

requirements:
  InitialWorkDirRequirement:
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
              "tck": ""
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
  ResourceRequirement:
    coresMin: 1
    ramMin: 2048

hints:
  $import: "type_dependencies/transformation/metadata-lhcb_simulate.yaml"

inputs:
  run-id:
    type: int
    default: 123
    inputBinding:
      prefix: "--run-id"
  task-id:
    type: int
    default: 456
    inputBinding:
      prefix: "--task-id"
  number-of-events:
    type: int
    default: 1
    inputBinding:
      prefix: "--number-of-events"

arguments:
  - position: 1
    valueFrom: "configuration.json"
  - prefix: "--pool-xml-catalog"
    valueFrom: "pool_xml_catalog.xml"

outputs:
  sim:
    type: File[]?
    outputBinding:
      glob: "*.sim"
  pool_xml_catalog:
    type: File?
    outputBinding:
      glob: "pool_xml_catalog.xml"
  others:
    type: File[]?
    outputBinding:
      glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "GeneratorLog.xml", "prmon.log", "Gauss*.log"]

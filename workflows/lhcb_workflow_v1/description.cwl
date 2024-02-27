cwlVersion: v1.2
class: Workflow
label: "LHCb MC workflow"
doc: >
  This workflow is composed of 2 main steps that should generate 2 types of jobs:
  * MCSimulation: Gauss execution
  * MCReconstruction: Boole, Moore, Brunel and DaVinci executions based on Gauss outputs

# Define the inputs of the workflow
inputs:
  run-id:
    type: string
  task-id:
    type: string
  # Theoretically, the parameters of all the steps should be defined here, it would generate a lot of parameters

# Define the outputs of the workflow
outputs:
  processing_results:
    type: File[]
    outputSource: reconstruction/results

# Requirements for the workflow
requirements:
  SubworkflowFeatureRequirement: {}

# Define the steps of the workflow
steps:
  # Simulation step
  simulation:
    in:
      run-id: run-id
      task-id: task-id
    out: [sim, pool_xml_catalog_name, prodconf_json, prodconf_py]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 4
          ramMin: 2048

      inputs:
        # Positional inputs
        application-name:
          type: string
          default: "Gauss"
          inputBinding:
            position: 1
        application-version:
          type: string
          default: "v56r7"
          inputBinding:
            position: 2
        options:
          type: string
          default: "$APPCONFIGOPTS/Gauss/Beam6500GeV-mu100-2016-nu1.6.py;$APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py;$APPCONFIGOPTS/Gauss/DataType-2016.py;$APPCONFIGOPTS/Gauss/RICHRandomHits.py;$DECFILESROOT/options/15808000.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmOpt2.py;$APPCONFIGOPTS/Persistency/BasketSize-10.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
          inputBinding:
            position: 3
        run-id:
          type: string
          inputBinding:
            position: 5
        task-id:
          type: string
          inputBinding:
            position: 6
        output-types:
          type: string
          default: "sim"
          inputBinding:
            position: 7
        # Named inputs
        extra-packages:
          type: string[]
          default: ["AppConfig.v3r425", "Gen/DecFiles.v32r18"]
        system-config:
          type: string
          default: "x86_64_v2-centos7-gcc11-opt"
        event-timeout:
          type: int
          default: 3600
        number-of-processors:
          type: int
          default: 1
        nightly:
          type: string?
        options-format:
          type: string?
        gaudi-options:
          type: string[]?
        gaudi-extra-options:
          type: string?
        processing-pass:
          type: string?
        mc-tck:
          type: string?
        run-number:
          type: int?
        histogram:
          type: boolean
          default: true
        dddb-tag:
          type: string
          default: "dddb-20220927-2016"
        online-ddb-tag:
          type: string?
        conddb-tag:
          type: string
          default: "sim-20201113-6-vc-mu100-Sim10"
        online-conddb-tag:
          type: string?
        dq-tag:
          type: string?
        step-id:
          type: int
          default: 0
        # Gauss specific options
        number-of-events:
          type: int
          default: 2
        use-prmon:
          type: boolean
          default: false

      outputs:
        sim:
          type: File[]
          outputBinding:
            glob: "*.sim"
        pool_xml_catalog_name:
          type: File
          outputBinding:
            glob: "pool_xml_catalog.xml"
        prodconf_json:
          type: File
          outputBinding:
            glob: "prodConf*.json"
        prodconf_py:
          type: File
          outputBinding:
            glob: "prodConf*.py"
      baseCommand: [lhcb_app.py]

  # Reconstruction step
  reconstruction:
    in:
      run-id: run-id
      task-id: task-id
      inputs: simulation/sim
      pool-xml-catalog-name: simulation/pool_xml_catalog_name
    out: [results]
    run:
      class: Workflow
      requirements:
        ResourceRequirement:
          coresMin: 1
          coresMax: 3
          ramMin: 2048
          ramMax: 4096

      inputs:
        run-id:
          type: string
        task-id:
          type: string
        inputs:
          type: File[]
        pool-xml-catalog-name:
          type: File

      outputs:
        results:
          type: File[]

      steps:
        # Boole step
        digitization:
          in:
            run-id: run-id
            task-id: task-id
            inputs: inputs
            pool-xml-catalog-name: pool-xml-catalog-name
          out: [digi]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              # Positional inputs
              application-name:
                type: string
                default: "Boole"
                inputBinding:
                  position: 1
              application-version:
                type: string
                default: "v30r4"
                inputBinding:
                  position: 2
              options:
                type: string
                default: "$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/EnableSpillover.py;$APPCONFIGOPTS/Boole/DataType-2015.py;$APPCONFIGOPTS/Boole/Boole-SetOdinRndTrigger.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                inputBinding:
                  position: 3
              run-id:
                type: string
                inputBinding:
                  position: 5
              task-id:
                type: string
                inputBinding:
                  position: 6
              output-types:
                type: string
                default: "digi"
                inputBinding:
                  position: 7
              # Named inputs
              pool-xml-catalog-name:
                type: File
              extra-packages:
                type: string[]
                default: ["AppConfig.v3r425"]
              system-config:
                type: string
                default: "any"
              event-timeout:
                type: int
                default: 3600
              number-of-processors:
                type: int
                default: 1
              nightly:
                type: string?
              options-format:
                type: string?
              gaudi-options:
                type: string[]?
              gaudi-extra-options:
                type: string?
              processing-pass:
                type: string?
              inputs:
                type: File[]
                inputBinding:
                  separate: true
              mc-tck:
                type: string?
              histogram:
                type: boolean?
              dddb-tag:
                type: string
                default: "dddb-20170721-3"
              online-ddb-tag:
                type: string?
              conddb-tag:
                type: string
                default: "sim-20170721-2-vc-mu100"
              online-conddb-tag:
                type: string?
              dq-tag:
                type: string?
              step-id:
                type: int
                default: 1

            outputs:
              digi:
                type: File[]
                outputBinding:
                  glob: "*1.digi"

        # Moore step 1
        init_reconstruction_1:
          in:
            run-id: run-id
            task-id: task-id
            inputs: [digitization/digi]
            pool-xml-catalog-name: pool-xml-catalog-name
          out: [digi]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              # Positional inputs
              application-name:
                type: string
                default: "Moore"
                inputBinding:
                  position: 1
              application-version:
                type: string
                default: "v25r5p3"
                inputBinding:
                  position: 2
              options:
                type: string
                default: "$APPCONFIGOPTS/L0App/L0AppSimProduction.py;$APPCONFIGOPTS/L0App/L0AppTCK-0x160F.py;$APPCONFIGOPTS/L0App/ForceLUTVersionV8.py;$APPCONFIGOPTS/L0App/DataType-2016.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                inputBinding:
                  position: 3
              run-id:
                type: string
                inputBinding:
                  position: 5
              task-id:
                type: string
                inputBinding:
                  position: 6
              output-types:
                type: string
                default: "digi"
                inputBinding:
                  position: 7
              # Named inputs
              pool-xml-catalog-name:
                type: File
              extra-packages:
                type: string[]
                default: ["AppConfig.v3r425"]
              system-config:
                type: string
                default: "any"
              event-timeout:
                type: int
                default: 3600
              number-of-processors:
                type: int
                default: 1
              nightly:
                type: string?
              options-format:
                type: string
                default: "l0app"
              gaudi-options:
                type: string[]?
              gaudi-extra-options:
                type: string?
              processing-pass:
                type: string?
              inputs:
                type: File[]
                inputBinding:
                  separate: true
              mc-tck:
                type: string?
              run-number:
                type: int?
              histogram:
                type: boolean?
              dddb-tag:
                type: string
                default: "dddb-20170721-3"
              online-ddb-tag:
                type: string?
              conddb-tag:
                type: string
                default: "sim-20170721-2-vc-mu100"
              online-conddb-tag:
                type: string?
              dq-tag:
                type: string?
              step-id:
                type: int
                default: 2

            outputs:
              digi:
                type: File[]
                outputBinding:
                  glob: "*2.digi"

        # Moore step 2
        init_reconstruction_2:
          in:
            run-id: run-id
            task-id: task-id
            inputs: [init_reconstruction_1/digi]
            pool-xml-catalog-name: pool-xml-catalog-name
          out: [digi]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              # Positional inputs
              application-name:
                type: string
                default: "Moore"
                inputBinding:
                  position: 1
              application-version:
                type: string
                default: "v25r5p3"
                inputBinding:
                  position: 2
              options:
                type: string
                default: "$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep2015.py;$APPCONFIGOPTS/Conditions/TCK-0x5138160F.py;$APPCONFIGOPTS/Moore/DataType-2016.py;$APPCONFIGOPTS/Moore/MooreSimProductionHlt1.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                inputBinding:
                  position: 3
              run-id:
                type: string
                inputBinding:
                  position: 5
              task-id:
                type: string
                inputBinding:
                  position: 6
              output-types:
                type: string
                default: "digi"
                inputBinding:
                  position: 7
              # Named inputs
              pool-xml-catalog-name:
                type: File
              extra-packages:
                type: string[]
                default: ["AppConfig.v3r425"]
              system-config:
                type: string
                default: "any"
              event-timeout:
                type: int
                default: 3600
              number-of-processors:
                type: int
                default: 1
              nightly:
                type: string?
              options-format:
                type: string?
              gaudi-options:
                type: string[]?
              gaudi-extra-options:
                type: string?
              processing-pass:
                type: string?
              inputs:
                type: File[]
                inputBinding:
                  separate: true
              mc-tck:
                type: string?
              histogram:
                type: boolean?
              dddb-tag:
                type: string
                default: "dddb-20170721-3"
              online-ddb-tag:
                type: string?
              conddb-tag:
                type: string
                default: "sim-20170721-2-vc-mu100"
              online-conddb-tag:
                type: string?
              dq-tag:
                type: string?
              step-id:
                type: int
                default: 3

            outputs:
              digi:
                type: File[]
                outputBinding:
                  glob: "*3.digi"

        # Moore step 3
        init_reconstruction_3:
          in:
            run-id: run-id
            task-id: task-id
            inputs: [init_reconstruction_2/digi]
            pool-xml-catalog-name: pool-xml-catalog-name
          out: [digi]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              # Positional inputs
              application-name:
                type: string
                default: "Moore"
                inputBinding:
                  position: 1
              application-version:
                type: string
                default: "v25r5p3"
                inputBinding:
                  position: 2
              options:
                type: string
                default: "$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep2015.py;$APPCONFIGOPTS/Conditions/TCK-0x6139160F.py;$APPCONFIGOPTS/Moore/DataType-2016.py;$APPCONFIGOPTS/Moore/MooreSimProductionHlt2.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                inputBinding:
                  position: 3
              run-id:
                type: string
                inputBinding:
                  position: 5
              task-id:
                type: string
                inputBinding:
                  position: 6
              output-types:
                type: string
                default: "digi"
                inputBinding:
                  position: 7
              # Named inputs
              pool-xml-catalog-name:
                type: File
              extra-packages:
                type: string[]
                default: ["AppConfig.v3r425"]
              system-config:
                type: string
                default: "any"
              event-timeout:
                type: int
                default: 3600
              number-of-processors:
                type: int
                default: 1
              nightly:
                type: string?
              options-format:
                type: string?
              gaudi-options:
                type: string[]?
              gaudi-extra-options:
                type: string?
              processing-pass:
                type: string?
              inputs:
                type: File[]
                inputBinding:
                  separate: true
              mc-tck:
                type: string?
              histogram:
                type: boolean?
              dddb-tag:
                type: string
                default: "dddb-20170721-3"
              online-ddb-tag:
                type: string?
              conddb-tag:
                type: string
                default: "sim-20170721-2-vc-mu100"
              online-conddb-tag:
                type: string?
              dq-tag:
                type: string?
              step-id:
                type: int
                default: 4

            outputs:
              digi:
                type: File[]
                outputBinding:
                  glob: "*4.digi"

        # Brunel
        full_event_reconstruction:
          in:
            run-id: run-id
            task-id: task-id
            inputs: [init_reconstruction_3/digi]
            pool-xml-catalog-name: pool-xml-catalog-name
          out: [dst]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              # Positional inputs
              application-name:
                type: string
                default: "Brunel"
                inputBinding:
                  position: 1
              application-version:
                type: string
                default: "v50r7"
                inputBinding:
                  position: 2
              options:
                type: string
                default: "$APPCONFIGOPTS/Brunel/DataType-2016.py;$APPCONFIGOPTS/Brunel/MC-WithTruth.py;$APPCONFIGOPTS/Brunel/SplitRawEventOutput.4.3.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                inputBinding:
                  position: 3
              run-id:
                type: string
                inputBinding:
                  position: 5
              task-id:
                type: string
                inputBinding:
                  position: 6
              output-types:
                type: string
                default: "dst"
                inputBinding:
                  position: 7
              # Named inputs
              pool-xml-catalog-name:
                type: File
              extra-packages:
                type: string[]
                default: ["AppConfig.v3r425"]
              system-config:
                type: string
                default: "any"
              event-timeout:
                type: int
                default: 3600
              number-of-processors:
                type: int
                default: 1
              nightly:
                type: string?
              options-format:
                type: string?
              gaudi-options:
                type: string[]?
              gaudi-extra-options:
                type: string?
              processing-pass:
                type: string?
              inputs:
                type: File[]
                inputBinding:
                  separate: true
              mc-tck:
                type: string?
              histogram:
                type: boolean?
              dddb-tag:
                type: string
                default: "dddb-20170721-3"
              online-ddb-tag:
                type: string?
              conddb-tag:
                type: string
                default: "sim-20170721-2-vc-mu100"
              online-conddb-tag:
                type: string?
              dq-tag:
                type: string?
              step-id:
                type: int
                default: 5

            outputs:
              dst:
                type: File[]
                outputBinding:
                  glob: "*5.dst"

        # DaVinci step 1
        analysis_1:
          in:
            run-id: run-id
            task-id: task-id
            inputs: [full_event_reconstruction/dst]
            pool-xml-catalog-name: pool-xml-catalog-name
          out: [dst]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              # Positional inputs
              application-name:
                type: string
                default: "Brunel"
                inputBinding:
                  position: 1
              application-version:
                type: string
                default: "v41r5"
                inputBinding:
                  position: 2
              options:
                type: string
                default: "$APPCONFIGOPTS/Turbo/Tesla_2016_LinesFromStreams_MC.py;$APPCONFIGOPTS/Turbo/Tesla_PR_Truth_2016.py;$APPCONFIGOPTS/Turbo/Tesla_Simulation_2016.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                inputBinding:
                  position: 3
              run-id:
                type: string
                inputBinding:
                  position: 5
              task-id:
                type: string
                inputBinding:
                  position: 6
              output-types:
                type: string
                default: "dst"
                inputBinding:
                  position: 7
              # Named inputs
              pool-xml-catalog-name:
                type: File
              extra-packages:
                type: string[]
                default: ["AppConfig.v3r425","TurboStreamProd.v4r2p9"]
              system-config:
                type: string
                default: "any"
              event-timeout:
                type: int
                default: 3600
              number-of-processors:
                type: int
                default: 1
              nightly:
                type: string?
              options-format:
                type: string
                default: "Tesla"
              gaudi-options:
                type: string[]?
              gaudi-extra-options:
                type: string?
              processing-pass:
                type: string?
              inputs:
                type: File[]
                inputBinding:
                  separate: true
              mc-tck:
                type: string?
              histogram:
                type: boolean?
              dddb-tag:
                type: string
                default: "dddb-20170721-3"
              online-ddb-tag:
                type: string?
              conddb-tag:
                type: string
                default: "sim-20170721-2-vc-mu100"
              online-conddb-tag:
                type: string?
              dq-tag:
                type: string?
              step-id:
                type: int
                default: 6

            outputs:
              dst:
                type: File[]
                outputBinding:
                  glob: "*6.dst"

        # DaVinci step 2
        analysis_2:
          in:
            run-id: run-id
            task-id: task-id
            inputs: [full_event_reconstruction/dst]
            pool-xml-catalog-name: pool-xml-catalog-name
          out: [dst]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              # Positional inputs
              application-name:
                type: string
                default: "Brunel"
                inputBinding:
                  position: 1
              application-version:
                type: string
                default: "v44r11p6"
                inputBinding:
                  position: 2
              options:
                type: string
                default: "$RDCONFIGOPTS/FilterTau2MuPhi-Stripping28r2p2.py;$APPCONFIGOPTS/DaVinci/DV-RedoCaloPID-Stripping_28_24.py;$APPCONFIGOPTS/DaVinci/DataType-2016.py;$APPCONFIGOPTS/DaVinci/InputType-DST.py;$APPCONFIGOPTS/DaVinci/DV-RawEventJuggler-4_3-to-4_3.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
                inputBinding:
                  position: 3
              run-id:
                type: string
                inputBinding:
                  position: 5
              task-id:
                type: string
                inputBinding:
                  position: 6
              output-types:
                type: string
                default: "tau2muphi.strip.dst"
                inputBinding:
                  position: 7
              # Named inputs
              pool-xml-catalog-name:
                type: File
              extra-packages:
                type: string[]
                default: ["AppConfig.v3r425","WG/RDConfig.v1r119"]
              system-config:
                type: string
                default: "any"
              event-timeout:
                type: int
                default: 3600
              number-of-processors:
                type: int
                default: 1
              nightly:
                type: string?
              options-format:
                type: string?
              gaudi-options:
                type: string[]?
              gaudi-extra-options:
                type: string?
              processing-pass:
                type: string?
              inputs:
                type: File[]
                inputBinding:
                  separate: true
              mc-tck:
                type: string?
              histogram:
                type: boolean?
              dddb-tag:
                type: string
                default: "dddb-20170721-3"
              online-ddb-tag:
                type: string?
              conddb-tag:
                type: string
                default: "sim-20170721-2-vc-mu100"
              online-conddb-tag:
                type: string?
              dq-tag:
                type: string?
              step-id:
                type: int
                default: 7

            outputs:
              dst:
                type: File[]
                outputBinding:
                  glob: "*7.dst"

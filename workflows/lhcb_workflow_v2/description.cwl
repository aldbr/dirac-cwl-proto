cwlVersion: v1.2
class: Workflow
label: "LHCb MC workflow"
doc: >
  This workflow is composed of 2 main steps that should generate 2 types of jobs:
  * MCSimulation: Gauss execution
  * MCReconstruction: Boole, Moore, Brunel and DaVinci executions based on Gauss outputs

# Define the inputs of the workflow
inputs:
  configuration-gauss:
    type: File
  configuration-boole:
    type: File
  configuration-moore-1:
    type: File
  configuration-moore-2:
    type: File
  configuration-moore-3:
    type: File
  configuration-brunel:
    type: File
  configuration-davinci-1:
    type: File
  configuration-davinci-2:
    type: File

# Define the outputs of the workflow
outputs:
  simulation_results:
    type: File[]?
    outputSource: simulation/sim
  simulation_others:
    type: File[]?
    outputSource: simulation/others
  reconstruction_results:
    type: File[]?
    outputSource: reconstruction/results
  reconstruction_others:
    type: File[]?
    outputSource: reconstruction/others

# Requirements for the workflow
requirements:
  SubworkflowFeatureRequirement: {}

# Define the steps of the workflow
steps:
  # Simulation step
  simulation:
    in:
      configuration: configuration-gauss
    out: [sim, pool_xml_catalog, others]
    run:
      class: CommandLineTool
      requirements:
        ResourceRequirement:
          coresMin: 4
          ramMin: 2048

      inputs:
        configuration:
          type: File
          inputBinding:
            position: 1

      arguments:
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
      baseCommand: [lhcb_app.py]

  # Reconstruction step
  reconstruction:
    in:
      configuration-boole: configuration-boole
      configuration-moore-1: configuration-moore-1
      configuration-moore-2: configuration-moore-2
      configuration-moore-3: configuration-moore-3
      configuration-brunel: configuration-brunel
      configuration-davinci-1: configuration-davinci-1
      configuration-davinci-2: configuration-davinci-2
      files: simulation/sim
      pool-xml-catalog: simulation/pool_xml_catalog
    out:
      - results
      - others
    run:
      class: Workflow
      requirements:
        MultipleInputFeatureRequirement: {}
        ResourceRequirement:
          coresMin: 1
          coresMax: 3
          ramMin: 2048
          ramMax: 4096

      inputs:
        configuration-boole:
          type: File
        configuration-moore-1:
          type: File
        configuration-moore-2:
          type: File
        configuration-moore-3:
          type: File
        configuration-brunel:
          type: File
        configuration-davinci-1:
          type: File
        configuration-davinci-2:
          type: File
        files:
          type: File[]?
        pool-xml-catalog:
          type: File?

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
            configuration: configuration-boole
            files: files
            pool-xml-catalog: pool-xml-catalog
          out: [digi, others]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              configuration:
                type: File
                inputBinding:
                  position: 1
              files:
                type: File[]?
                inputBinding:
                  prefix: "--files"
              pool-xml-catalog:
                type: File?
                inputBinding:
                  prefix: "--pool-xml-catalog"

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
            configuration: configuration-moore-1
            files: [digitization/digi]
            pool-xml-catalog: pool-xml-catalog
          out: [digi, others]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              configuration:
                type: File
                inputBinding:
                  position: 1
              files:
                type: File[]?
                inputBinding:
                  prefix: "--files"
              pool-xml-catalog:
                type: File?
                inputBinding:
                  prefix: "--pool-xml-catalog"

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
            configuration: configuration-moore-2
            files: [init_reconstruction_1/digi]
            pool-xml-catalog: pool-xml-catalog
          out: [digi, others]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              configuration:
                type: File
                inputBinding:
                  position: 1
              files:
                type: File[]?
                inputBinding:
                  prefix: "--files"
              pool-xml-catalog:
                type: File?
                inputBinding:
                  prefix: "--pool-xml-catalog"

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
            configuration: configuration-moore-3
            files: [init_reconstruction_2/digi]
            pool-xml-catalog: pool-xml-catalog
          out: [digi, others]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              configuration:
                type: File
                inputBinding:
                  position: 1
              files:
                type: File[]?
                inputBinding:
                  prefix: "--files"
              pool-xml-catalog:
                type: File?
                inputBinding:
                  prefix: "--pool-xml-catalog"

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
            configuration: configuration-brunel
            files: [init_reconstruction_3/digi]
            pool-xml-catalog: pool-xml-catalog
          out: [dst, others]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              configuration:
                type: File
                inputBinding:
                  position: 1
              files:
                type: File[]?
                inputBinding:
                  prefix: "--files"
              pool-xml-catalog:
                type: File?
                inputBinding:
                  prefix: "--pool-xml-catalog"

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
            configuration: configuration-davinci-1
            files: [full_event_reconstruction/dst]
            pool-xml-catalog: pool-xml-catalog
            secondary-files: [init_reconstruction_3/digi]
          out: [dst, others]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              configuration:
                type: File
                inputBinding:
                  position: 1
              files:
                type: File[]?
                inputBinding:
                  prefix: "--files"
              pool-xml-catalog:
                type: File?
                inputBinding:
                  prefix: "--pool-xml-catalog"
              secondary-files:
                type: File[]?
                inputBinding:
                  prefix: "--secondary-files"

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
            configuration: configuration-davinci-2
            files: [analysis_1/dst]
            pool-xml-catalog: pool-xml-catalog
          out: [dst, others]
          run:
            class: CommandLineTool
            baseCommand: [lhcb_app.py]

            inputs:
              configuration:
                type: File
                inputBinding:
                  position: 1
              files:
                type: File[]?
                inputBinding:
                  prefix: "--files"
              pool-xml-catalog:
                type: File?
                inputBinding:
                  prefix: "--pool-xml-catalog"

            outputs:
              dst:
                type: File[]?
                outputBinding:
                  glob: "*.Tau2MuPhi.Strip.dst"
              others:
                type: File[]?
                outputBinding:
                  glob: ["prodConf*.json", "prodConf*.py", "summary*.xml", "prmon.log", "DaVinci*.log"]

#!/usr/bin/env python3
from ProductionRequestToCWL import fromProductionRequestYAMLToCWL
from pathlib import Path
from cwl_utils.parser import save
from ruamel.yaml import YAML

workflow, inputs, metadata = fromProductionRequestYAMLToCWL(
    Path('production_requests/simulation.yaml'),
    production_name='RD_Lb2pKpipiMuMu_Run2_Run3 2016 pp MagUp',
    event_type='15146001'
)

workflow_dict = save(workflow)

# Use ruamel.yaml to preserve LiteralScalarString formatting
yaml = YAML()
yaml.default_flow_style = False
yaml.width = 120
with open('generated_test.cwl', 'w') as f:
    yaml.dump(workflow_dict, f)

print('✅ Generated: generated_test.cwl')
print(f'✅ Steps: {len(workflow.steps)}')

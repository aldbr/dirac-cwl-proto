# dirac cwl prototype

This is a prototype for managing CWL workflows and related tools.

## Installation

To use this package, you first need to create a conda environment:

```bash
mamba env create -f environment.yaml
conda activate dirac-cwl
```

Then, install the package:

```bash
pip install -e .
```

## Usage

```bash
dirac-cwl <workflow_path> <input_path>
```

This package contains modules and tools to manage CWL workflows:

- `src/modules`: Python scripts for individual steps in workflows.
- `src/cli`: Utility scripts for managing and executing CWL workflows.
- `workflows`: CWL workflow definitions.

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

This package contains modules and tools to manage CWL workflows:

- `src/modules`: Python scripts for individual steps in workflows.
- `src/cli`: Utility scripts for managing and executing CWL workflows.
- `workflows`: CWL workflow definitions.


## Design choices

> Why do I need to define sub-workflows when defining a production?

`CWL` does not support sub-workflow definitions within the main workflow: [ref](https://www.commonwl.org/user_guide/topics/workflows.html#workflows)
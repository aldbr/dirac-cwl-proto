# dirac cwl prototype

This Python prototype provides a command-line interface (CLI) for the end-to-end execution of Common Workflow Language (CWL) workflows. It enables users to locally test CWL workflows, submit them as jobs to the DIRAC Workload Management System (WMS) for execution on remote computing resources, and manage large-scale productions by splitting a workflow into multiple transformations.


- Local Workflow Testing: Initially, the user tests the CWL workflow locally using `cwltool`. This step involves validating the workflow's structure and ensuring that it executes correctly with the provided inputs.

- Submission as "DIRAC" Jobs: Once the workflow passes local testing, it can be submitted to "DIRAC" WMS as a job. This allows the workflow to be executed on remote computing resources, leveraging DIRAC's distributed computing capabilities. The application automates the process of job creation, submission, and monitoring.

- Submission as "DIRAC" Productions: For workflows requiring large-scale execution, such as those involving Monte Carlo simulations, the application provides a mechanism to split the workflow into multiple transformations. Each transformation represents a discrete step in the workflow and is capable of executing one or more jobs. This approach facilitates parallel processing, significantly reducing the overall execution time and improving resource efficiency.

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
dirac-cwl <workflow_path> <input_path> <metadata model>
```

This package contains modules and tools to manage CWL workflows:

- `src/modules`: Python scripts for individual steps in workflows.
- `src/cli`: Utility scripts for managing and executing CWL workflows.
- `workflows`: CWL workflow definitions.

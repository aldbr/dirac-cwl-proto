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
dirac-cwl production submit <workflow_path> <input_path> <metadata model>
```

This package contains modules and tools to manage CWL workflows:

- `src/modules`: Python scripts for individual steps in workflows.
- `src/cli`: Utility scripts for managing and executing CWL workflows.
- `workflows`: CWL workflow definitions.

To use the workflows and inputs directly with `cwltool`, you need to add the `modules` directory to the `$PATH`:

```bash
export PATH=$PATH:</path/to/dirac-cwl-proto/src/dirac_cwl_proto/modules>
cwltool <workflow_path> <inputs>
```

## Contribute

### Add a workflow

To add a new workflow to the project, follow these steps:

- Create a new directory under `workflows`
- Add required files: Ensure that the directory contains at least the following two files:
    - `description.cwl`: This is the CWL (Common Workflow Language) file that describes the workflow.
    - `inputs.yml`: This YAML file contains the inputs that will be provided to the CWL workflow.

Directory Structure Example:

```
workflows/
├── my_new_workflow/
│   ├── description.cwl
│   └── inputs.yml
```

### Add a module

If your workflow requires calling a script, you can add this script as a module. Follow these steps to properly integrate the module:

- Add the script: Place your script in the `src/dirac_cwl_proto/modules` directory.
- Update `pyproject.toml`: Add the script to the `pyproject.toml` file to create a command-line interface (CLI) command.
- Reinstall the package: Run `pip install .` to reinstall the package and make the new script available as a command.
- Usage in CWL Workflow: Reference the command in your `description.cwl` file.

**Example**

Let’s say you have a script named `generic_command.py` located at `src/dirac_cwl_proto/modules/generic_command.py`. Here's how you can integrate it:

- `generic_command.py` Example Script:

```python
#!/usr/bin/env python3
import typer
from rich.console import Console

app = typer.Typer()
console = Console()

@app.command()
def run_example():
    console.print("This is an example command.")

if __name__ == "__main__":
    app()
```

- Update `pyproject.toml`:

```toml
[project.scripts]
generic-command = "dirac_cwl_proto.modules.generic_command:app"
```

- Reinstall the package with `pip install .`:
- Reference in description.cwl:

```yaml
baseCommand: [generic-command]
```

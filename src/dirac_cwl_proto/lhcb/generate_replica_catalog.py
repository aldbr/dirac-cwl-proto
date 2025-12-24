
import json
import logging
import random
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal, Self

import typer
import yaml
from pydantic import BaseModel, Field, StringConstraints, model_validator
from rich.console import Console
from rich.logging import RichHandler

# Setup logging with rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
)
logger = logging.getLogger(__name__)
console = Console()


def _format_bkquery_param(runs):
    # This became necessary because DIRAC cannot handle
    # lists of int, only list of str.
    if runs:
        return [str(r) for r in runs]
    return None

class DataQualityFlag(StrEnum):
    OK = "OK"
    BAD = "BAD"
    UNCHECKED = "UNCHECKED"

class InputDataset(BaseModel):
    """CWL hint for input dataset description."""

    class BookkeepingQuery(BaseModel):
        configName: str
        configVersion: str
        inFileType: str
        inProPass: str
        # TODO: Accept None and lists on these
        inDataQualityFlag: str | list[str] = "OK"
        inExtendedDQOK: Annotated[list[str], Field(min_length=1)] | None = None
        inProductionID: str = "ALL"
        inTCKs: str = "ALL"
        inSMOG2State: Annotated[list[str], Field(min_length=1)] | None = None

    conditions_dict: BookkeepingQuery
    conditions_description: str
    event_type: Annotated[str, StringConstraints(pattern=r"[0-9]{8}")]

    class LaunchParameters(BaseModel):
        """Extra info which is currently needed when making transformations from a request"""

        run_numbers: list[str] | None = None
        sample_max_md5: Annotated[str, StringConstraints(pattern=r"[A-Z0-9]{32}")] | None = None
        sample_seed_md5: Annotated[str, StringConstraints(pattern=r"[A-Z0-9]{32}")] | None = None
        start_run: int | None = None
        end_run: int | None = None

        @model_validator(mode="after")
        def validate_run_numbers(self) -> Self:
            if self.run_numbers and (self.start_run is not None or self.end_run is not None):
                raise ValueError("Cannot specify both run_numbers and start_run/end_run")
            return self

    launch_parameters: Annotated[LaunchParameters, Field(default_factory=LaunchParameters)]


    class_: Literal["dirac:inputDataset"] = Field(alias="class")

    def make_bkquery(self) -> dict[str, Any]:
        """Create BKQuery dict from InputDataset for querying the Bookkeeping.

        This transforms the CWL hint structure into the format expected by
        LHCbDIRAC's BookkeepingClient.getFilesWithMetadata().

        Returns:
            A dictionary suitable for BookkeepingClient BK queries.
        """
        # Required base fields
        bk_query_dict: dict[str, Any] = {
            "FileType": self.conditions_dict.inFileType,
            "EventType": str(self.event_type),
            "ConfigName": self.conditions_dict.configName,
            "ConfigVersion": self.conditions_dict.configVersion,
        }

        # Optional: Data taking conditions (sim condition or beam conditions)
        if self.conditions_description:
            bk_query_dict["DataTakingConditions"] = self.conditions_description

        # Optional: Processing pass
        if self.conditions_dict.inProPass:
            bk_query_dict["ProcessingPass"] = self.conditions_dict.inProPass

        # Optional: Data quality flag(s)
        # Can be a single string or a list of strings
        if self.conditions_dict.inDataQualityFlag:
            dq_flag = self.conditions_dict.inDataQualityFlag
            if isinstance(dq_flag, list):
                # List format: convert to list of strings
                bk_query_dict["DataQualityFlag"] = [str(f) for f in dq_flag]
            else:
                # Single value: convert to list with one element
                bk_query_dict["DataQualityFlag"] = [str(dq_flag)]

        # Optional: Extended DQ OK flags
        if self.conditions_dict.inExtendedDQOK:
            bk_query_dict["ExtendedDQOK"] = self.conditions_dict.inExtendedDQOK

        # Optional: Production ID filter
        if self.conditions_dict.inProductionID and self.conditions_dict.inProductionID != "ALL":
            bk_query_dict["ProductionID"] = self.conditions_dict.inProductionID

        # Optional: TCKs filter
        if self.conditions_dict.inTCKs and self.conditions_dict.inTCKs != "ALL":
            bk_query_dict["TCKs"] = self.conditions_dict.inTCKs

        # Optional: SMOG2 state
        if self.conditions_dict.inSMOG2State:
            bk_query_dict["SMOG2"] = self.conditions_dict.inSMOG2State

        # Launch parameters: sample selection
        if self.launch_parameters.sample_max_md5 and self.launch_parameters.sample_seed_md5:
            bk_query_dict["SampleMax"] = self.launch_parameters.sample_max_md5
            bk_query_dict["SampleSeedMD5"] = self.launch_parameters.sample_seed_md5

        # Launch parameters: run number selection
        # Validate that we don't mix run_numbers with start_run/end_run
        if self.launch_parameters.run_numbers and (
            self.launch_parameters.start_run is not None or self.launch_parameters.end_run is not None
        ):
            raise ValueError("Cannot specify both run_numbers and start_run/end_run")

        # Validate start_run <= end_run
        if (
            self.launch_parameters.start_run is not None
            and self.launch_parameters.end_run is not None
            and self.launch_parameters.end_run < self.launch_parameters.start_run
        ):
            raise ValueError(
                f"end_run ({self.launch_parameters.end_run}) must be >= start_run ({self.launch_parameters.start_run})"
            )

        # Add run number filters
        if self.launch_parameters.start_run is not None:
            bk_query_dict["StartRun"] = self.launch_parameters.start_run

        if self.launch_parameters.end_run is not None:
            bk_query_dict["EndRun"] = self.launch_parameters.end_run

        if self.launch_parameters.run_numbers:
            # Convert to list of strings if needed
            bk_query_dict["RunNumbers"] = [
                str(r) if not isinstance(r, str) else r for r in self.launch_parameters.run_numbers
            ]

        return bk_query_dict




def do_bkquery(input_dataset: InputDataset, inputFiles: list[str] | None = None, numTestLFNs: int = 1):
    """Query the Bookkeeping for LFNs matching the input dataset criteria.

    :param input_dataset: InputDataset model with BK query parameters
    :param inputFiles: Optional pre-selected list of LFNs (if provided, skip BK query)
    :param numTestLFNs: Number of LFNs to retrieve
    :return: List of LFNs with available replicas
    """
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

    bkQueryDict = input_dataset.make_bkquery()

    if not inputFiles:
        logger.info("Querying Bookkeeping for LFNs...")
        result = returnValueOrRaise(
            BookkeepingClient().getFilesWithMetadata(bkQueryDict | {"OnlyParameters": ["FileName", "FileSize"]})
        )
        if result["TotalRecords"] == 0:
            query_str = json.dumps(bkQueryDict, indent=4)
            raise ValueError(
                f"No input files found in the bookkeeping.\n\n"
                f"Bookkeeping query used:\n"
                f"\n{query_str}\n\n"
                f"Please verify that:\n"
                f"  - The bookkeeping path is correct\n"
                f"  - The requested runs (if specified) actually exist\n"
                f"  - The data quality flags are correct\n"
            )

        logger.info(f"Found {result['TotalRecords']} files in Bookkeeping")

        # Remove the smallest 50% of files to avoid unusually small files
        sizeIndex = result["ParameterNames"].index("FileSize")
        records = sorted(result["Records"], key=lambda x: x[sizeIndex])
        if len(records) // 2 >= numTestLFNs:
            records = records[len(records) // 2 :]
            logger.debug(f"Filtered out smallest 50% of files, {len(records)} remaining")

        # Shuffle the LFNs so we pick a random one
        random.shuffle(records)

        # Only run tests with files which have available replicas
        filenameIndex = result["ParameterNames"].index("FileName")
        inputFiles = []
        skipped_files = []
        logger.info(f"Checking for replicas on disk (need {numTestLFNs} files)...")
        for record in records:
            lfn = record[filenameIndex]
            replica_result = returnValueOrRaise(DataManager().getReplicasForJobs([lfn], diskOnly=True))
            inputFiles.extend(replica_result["Successful"])
            if len(inputFiles) == numTestLFNs:
                break
            if replica_result["Failed"]:
                skipped_files.extend(replica_result["Failed"])
                logger.warning(f"Skipping LFN (no disk replicas): {replica_result['Failed']}")
        else:
            error_msg = (
                f"Insufficient input files with available (disk) replicas for jobs found.\n\n"
                f"Summary:\n"
                f"  - Files requested for test: {numTestLFNs}\n"
                f"  - Files found with replicas: {len(inputFiles)}\n"
                f"  - Files skipped (no disk replicas): {len(skipped_files)}\n\n"
                f"This usually means the files need to be staged from tape.\n\n"
                f"Solutions:\n"
                f"  1. Request staging of the required files by contacting LHCb Data Management\n\n"
                f"Contact: lhcb-datamanagement@cern.ch"
            )
            if skipped_files:
                error_msg += f"\n\nExample skipped files:\n  " + "\n  ".join(skipped_files[:3])
                if len(skipped_files) > 3:
                    error_msg += f"\n  ... and {len(skipped_files) - 3} more"
            raise ValueError(error_msg)

        logger.info(f"Found {len(inputFiles)} files with available replicas")

    if len(inputFiles) < numTestLFNs:
        raise ValueError(
            f"Insufficient input files available.\n\n"
            f"Summary:\n"
            f"  - Files requested for test: {numTestLFNs}\n\n"
            f"  - Files available: {len(inputFiles)}\n"
            f"This could indicate that some files need to be staged from tape storage.\n\n"
            f"Solutions:\n"
            f"  1. Use --num-test-lfns={len(inputFiles)} to work with the available files\n"
            f"  2. Contact LHCb Data Management to request staging of additional files\n"
            f"Contact: lhcb-datamanagement@cern.ch"
        )

    return inputFiles


app = typer.Typer(
    help="Generate replica catalog and input YAML from CWL workflow with inputDataset hint",
    no_args_is_help=True,
)


@app.command()
def main(
    cwl_file: Path = typer.Argument(
        ...,
        help="Path to CWL file with inputDataset hint",
        exists=True,
        dir_okay=False,
        readable=True,
    ),
    n_lfns: int = typer.Option(
        1,
        "--n-lfns",
        "-n",
        help="Number of LFNs to retrieve from Bookkeeping",
    ),
    output_yaml: Path = typer.Option(
        None,
        "--output-yaml",
        "-o",
        help="Output path for CWL input YAML file (default: <cwl_file>-inputs.yml)",
    ),
    output_catalog: Path = typer.Option(
        None,
        "--output-catalog",
        "-c",
        help="Output path for replica catalog JSON (default: <cwl_file>-catalog.json)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
):
    """Generate replica catalog and input YAML from a CWL workflow.

    Reads the inputDataset hint from a CWL workflow file, queries the LHCb Bookkeeping
    for matching files with available replicas, and generates:

    1. A CWL input YAML file with the input-data parameter
    2. A replica catalog JSON file for local execution
    """
    if verbose:
        logger.setLevel(logging.DEBUG)

    # initialise dirac first
    import DIRAC
    DIRAC.initialize()

    # Set default output paths if not provided
    if output_yaml is None:
        output_yaml = cwl_file.parent / f"{cwl_file.stem}-inputs.yml"
    if output_catalog is None:
        output_catalog = cwl_file.parent / f"{cwl_file.stem}-catalog.json"

    # Read the CWL file and extract inputDataset hint
    logger.info(f"Reading CWL file: {cwl_file}")
    with open(cwl_file, "r") as f:
        cwl_content = yaml.safe_load(f)

    # Find the inputDataset hint
    hints = cwl_content.get("hints", [])
    input_dataset_dict = None
    for hint in hints:
        if hint.get("class") == "dirac:inputDataset":
            input_dataset_dict = hint
            break

    if not input_dataset_dict:
        console.print("[red]Error:[/red] No inputDataset hint found in CWL file")
        console.print("Expected a hint with class='dirac:inputDataset' in the 'hints' section")
        raise typer.Exit(1)

    # Parse into InputDataset model
    try:
        input_dataset = InputDataset(**input_dataset_dict)
    except Exception as e:
        console.print(f"[red]Error:[/red] Failed to parse inputDataset hint: {e}")
        raise typer.Exit(1)

    logger.info(f"Found inputDataset hint:")
    logger.info(f"  Event Type: {input_dataset.event_type}")
    logger.info(f"  Config: {input_dataset.conditions_dict.configName}/{input_dataset.conditions_dict.configVersion}")
    logger.info(f"  Processing Pass: {input_dataset.conditions_dict.inProPass}")

    # Query Bookkeeping for LFNs
    try:
        lfns_list = do_bkquery(input_dataset, numTestLFNs=n_lfns)
    except Exception as e:
        console.print(f"[red]Error:[/red] Bookkeeping query failed: {e}")
        raise typer.Exit(1)

    logger.info(f"Retrieved {len(lfns_list)} LFNs")

    # Generate replica catalog using DIRAC
    from dirac_cwl_proto.job.replica_catalog import dirac_make_replica_catalog

    logger.info("Generating replica catalog with DIRAC...")
    try:
        replica_catalog = dirac_make_replica_catalog(lfns_list)
    except Exception as e:
        console.print(f"[red]Error:[/red] Failed to generate replica catalog: {e}")
        raise typer.Exit(1)

    # Generate CWL input YAML file
    cwl_inputs = {
        "input-data": [
            {
                "class": "File",
                "location": f"LFN:{lfn}",
            }
            for lfn in lfns_list
        ]
    }

    logger.info(f"Writing CWL input YAML to: {output_yaml}")
    with open(output_yaml, "w") as f:
        yaml.dump(cwl_inputs, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Writing replica catalog to: {output_catalog}")
    with open(output_catalog, "w") as f:
        # Convert Pydantic model to dict for JSON serialization
        f.write(replica_catalog.model_dump_json(indent=2))

    console.print(f"\n[green]✓[/green] Successfully generated:")
    console.print(f"  • Input YAML: {output_yaml}")
    console.print(f"  • Replica catalog: {output_catalog}")
    console.print(f"\nRetrieved {len(lfns_list)} LFN(s)")


if __name__ == "__main__":
    app()

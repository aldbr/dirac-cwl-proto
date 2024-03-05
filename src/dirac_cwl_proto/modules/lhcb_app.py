#!/usr/bin/env python3
import asyncio
import json
import logging
import re
import shlex
import shutil
import subprocess
from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from LbProdRun.models import JobSpecV1
from pydantic import BaseModel, model_validator
from rich.console import Console


class ApplicationName(str, Enum):
    Boole = "Boole"
    Brunel = "Brunel"
    DaVinci = "DaVinci"
    Gauss = "Gauss"
    LHCb = "LHCb"  # Merge
    Moore = "Moore"


class ApplicationConfiguration(BaseModel):
    """Application configuration."""

    name: ApplicationName
    version: str
    event_timeout: int = 3600
    extra_packages: Optional[list[str]] = []
    nightly: Optional[str] = None
    number_of_processors: int = 1
    system_config: str = "best"
    use_prmon: bool = False


class InputConfiguration(BaseModel):
    """Input configuration."""

    files: Optional[list[str]] = None
    secondary_files: Optional[list[str]] = None
    tck: Optional[str] = None
    mc_tck: Optional[str] = None
    pool_xml_catalog: str
    run_number: Optional[int] = None
    number_of_events: int = -1

    # validate that tck and mc_tck are not both set
    @model_validator(mode="after")
    def tck_xor_mc_tck(self):
        if self.tck and self.mc_tck:
            raise ValueError("TCK set, but shouldn't be!")
        return self


class OutputConfiguration(BaseModel):
    """Output configuration."""

    types: list[str]
    histogram: bool = False


class OptionsConfiguration(BaseModel):
    """Options configuration."""

    options: list[str] | dict[str, str]
    format: Optional[str] = None
    gaudi_options: Optional[list[str]] = None
    gaudi_extra_options: Optional[str] = None
    processing_pass: Optional[str] = None


class DBTagsConfiguration(BaseModel):
    """DB tags configuration."""

    dddb_tag: Optional[str] = None
    online_ddb_tag: Optional[str] = None
    conddb_tag: Optional[str] = None
    online_conddb_tag: Optional[str] = None
    dq_tag: Optional[str] = None

    # validate that dddb_tag and online_ddb_tag are not both set
    # validate that conddb_tag and online_conddb_tag are not both set
    @model_validator(mode="after")
    def db_tag_xor_online_ddb_tag(self):
        if self.dddb_tag and self.dddb_tag != "online" and self.online_ddb_tag:
            raise ValueError("DDDB tag set, but shouldn't be!")

        if self.conddb_tag and self.conddb_tag != "online" and self.online_conddb_tag:
            raise ValueError("Conddb tag set, but shouldn't be!")
        return self


class Configuration(BaseModel):
    """Configuration."""

    run_id: int
    task_id: int
    step_id: int
    application: ApplicationConfiguration
    input: InputConfiguration
    output: OutputConfiguration
    options: OptionsConfiguration
    db_tags: DBTagsConfiguration

    @model_validator(mode="after")
    def gaudi_required_fields(self):
        if self.application.name.value == ApplicationName.Gauss and not (
            self.run_id and self.task_id and self.input.number_of_events
        ):
            raise ValueError(
                "Simulation ID, Task ID and Number of Events are required for Gauss application"
            )
        if (
            self.application.name.value != ApplicationName.Gauss
            and not self.input.pool_xml_catalog
        ):
            raise ValueError("Pool XML catalog name is required")
        if self.application.name != ApplicationName.Gauss and not self.input.files:
            raise ValueError("Input data is required")
        return self


app = typer.Typer()
console = Console()


@app.command()
def run_application(
    app_config_path: str = typer.Argument(..., help="Application configuration"),
    files: Optional[list[str]] = typer.Option(
        None, help="List of input data files", show_default=False
    ),
    pool_xml_catalog: Optional[str] = typer.Option(
        None, help="Pool XML catalog name", show_default=False
    ),
    secondary_files: Optional[list[str]] = typer.Option(
        None,
        help="List of input data files that should be present in the working directory during the execution",
        show_default=False,
    ),
    run_id: Optional[int] = typer.Option(None, help="Simulation ID"),
    task_id: Optional[int] = typer.Option(None, help="Task ID"),
):
    """
    LHCb application: generates a prodconf.json file and runs the application.
    """
    console.print("[bold blue]Getting parameters...[/bold blue]")

    with open(app_config_path, "r") as f:
        app_config = json.load(f)

    # Override input files and pool_xml_catalog if provided
    if files:
        app_config["input"]["files"] = files
    if pool_xml_catalog:
        app_config["input"]["pool_xml_catalog"] = pool_xml_catalog
    if secondary_files:
        app_config["input"]["secondary_files"] = secondary_files
    if run_id:
        app_config["run_id"] = run_id
    if task_id:
        app_config["task_id"] = task_id

    configuration = Configuration.model_validate(app_config)

    # Check whether the execution needs hidden deps
    if not install_dependencies(configuration.input.secondary_files):
        console.print("[bold red]Failed to install dependencies[/bold red]")
        raise typer.Exit(code=1)

    # Get run & event number for Gauss application
    run_number_gauss, first_event_number_gauss = get_run_event_numbers(
        configuration.application.name,
        configuration.run_id,
        configuration.task_id,
        configuration.input.number_of_events,
    )
    if run_number_gauss and first_event_number_gauss:
        console.print(
            f"[bold blue]Run number: {run_number_gauss} & Event number: {first_event_number_gauss}[/bold blue]"
        )

    if configuration.options.gaudi_options:
        console.print("[bold blue]Building Gaudi extra options...[/bold blue]")
        # Prepare standard project run time options
        generated_options = build_gaudi_extra_options(
            gaudi_options=configuration.options.gaudi_options,
            application_name=configuration.application.name,
            number_of_events=configuration.input.number_of_events,
            pool_xml_catalog=configuration.input.pool_xml_catalog,
            run_number=run_number_gauss,
            first_event_number=first_event_number_gauss,
            inputs=configuration.input.files,
        )
        # If not lbexec style
        if isinstance(configuration.options.options, list):
            configuration.options.options.append(generated_options)
        console.print("[bold blue]Gaudi extra options generated![/bold blue]")

    output_file_prefix = (
        f"{configuration.application.name.value}_"
        f"{configuration.run_id}_"
        f"{configuration.task_id}_"
        f"{configuration.step_id}"
    )

    console.print("[bold blue]Building prodconf.json file...[/bold blue]")
    prodconf_file = build_prodconf_json(
        configuration,
        output_file_prefix=output_file_prefix,
        first_event_number=first_event_number_gauss,
    )
    console.print(
        f"[bold blue]prodconf.json file generated:\n{prodconf_file}[/bold blue]"
    )

    console.print("[bold blue]Running lb-prod-run prodconf.json...[/bold blue]")
    try:
        status_code, stdout, stderr = asyncio.get_event_loop().run_until_complete(
            run_lbprodrun(
                application_name=configuration.application.name,
                output_file_prefix=output_file_prefix,
                prodconf_file=prodconf_file,
                use_prmon=configuration.application.use_prmon,
            )
        )
    except RuntimeError as e:
        console.print(
            f"[bold red]{configuration.application.name} {configuration.application.version} Error: {e}[/bold red]"
        )
        raise typer.Exit(code=1) from e

    if status_code != 0:
        console.print(
            f"[bold red]{configuration.application.name} {configuration.application.version} "
            f"failed with status {status_code}: {stderr}[/bold red]"
        )
        raise typer.Exit(code=status_code)

    console.print(
        f"[bold green]lb-prod-run finished with status {status_code}[/bold green]"
    )


# ------------------------------------------------------------------------------
def install_dependencies(secondary_files):
    """Install dependencies.
    Basically check whether the files are present in the current working directory.
    If not, copy them locally."""
    if not secondary_files:
        return True

    for secondary_file in secondary_files:
        secondary_file_path = Path(secondary_file)
        if not secondary_file_path.is_file():
            logging.error(f"File {secondary_file} does not exist")
            return False

        # Already present in the current working directory
        if Path(secondary_file_path.name).is_file():
            continue

        # Copy the file to the current working directory
        logging.info(f"Copying {secondary_file} to the current working directory")
        shutil.copy(secondary_file, ".")
    return True


def get_run_event_numbers(
    application_name: ApplicationName,
    run_id: int,
    task_id: int,
    number_of_events: int,
) -> tuple:
    """
    Get the run and event numbers for Gauss application
    """
    run_number_gauss = None
    first_event_number_gauss = None

    if application_name.value == ApplicationName.Gauss:
        run_number_gauss = run_id * 100 + task_id
        first_event_number_gauss = number_of_events * (task_id - 1) + 1
    return run_number_gauss, first_event_number_gauss


# ------------------------------------------------------------------------------


def build_gaudi_extra_options(
    gaudi_options: list,
    application_name: ApplicationName,
    number_of_events: int,
    pool_xml_catalog: str,
    run_number: int | None,
    first_event_number: int | None,
    inputs: list[str] | None,
) -> str:
    """
    Build the Gaudi extra options
    """
    input_data_options = get_data_options(inputs, pool_xml_catalog)
    projectOpts = get_module_options(
        application_name=application_name,
        number_of_events=number_of_events,
        input_data_options=input_data_options,
        extra_options=gaudi_options,
        run_number=run_number,
        first_event_number=first_event_number,
    )
    logging.info(f"Project options: {projectOpts}")

    generated_options_file = "gaudi_extra_options.py"
    with open(generated_options_file, "w") as options:
        options.write(projectOpts)
    return generated_options_file


def get_data_options(inputs: list[str] | None, pool_xml_catalog: str) -> list[str]:
    """Given a list of input data and a specified input data type this function
    will return the correctly formatted EventSelector options for Gaudi
    applications specified by name.

    The options are returned as a python list.
    """
    options = []
    if inputs:
        input_data_options = get_event_selector_input(inputs)
        event_selector_options = f"""EventSelector().Input=[{input_data_options}];\n"""
        options.append(event_selector_options)

    pool_options = (
        f"""\nFileCatalog().Catalogs= ["xmlcatalog_file:{pool_xml_catalog}"]\n"""
    )
    options.append(pool_options)
    return options


def get_event_selector_input(input_data: list[str]) -> str:
    """Returns the correctly formatted event selector options for accessing input
    data using Gaudi applications."""
    options = []
    for lfn in input_data:
        lfn = lfn.replace("LFN:", "").replace("lfn:", "")

        data_type = lfn.split(".")[-1]
        if data_type == "MDF":
            options.append(f""" "DATAFILE='LFN:{lfn}' SVC='LHCb::MDFSelector'", """)
        elif data_type in ("ETC", "SETC", "FETC"):
            cmd = f"COLLECTION='TagCreator/EventTuple' DATAFILE='LFN:{lfn}' "
            cmd += "TYP='POOL_ROOT' SEL='(StrippingGlobal==1)' OPT='READ'"
            options.append(f""" {cmd} """)
        elif data_type == "RDST":
            if re.search("rdst$", lfn):
                options.append(
                    f""" "DATAFILE='LFN:{lfn}' TYP='POOL_ROOTTREE' OPT='READ'", """
                )
            else:
                logging.info(
                    f"Ignoring file {lfn} for step with input data type {data_type}"
                )
        else:
            options.append(
                f""" "DATAFILE='LFN:{lfn}' TYP='POOL_ROOTTREE' OPT='READ'", """
            )

    return "\n".join(options)[:-2]


def get_module_options(
    application_name: ApplicationName,
    number_of_events: int,
    input_data_options: list[str],
    extra_options: list[str] | None,
    run_number: int | None = 0,
    first_event_number: int | None = 1,
):
    """Return the standard options for a Gaudi application project to be used at
    run time by the workflow modules.

    The input data options field is a python list (output of
    getInputDataOptions() below). The runNumber and firstEventNumber only
    apply in the Gauss case and when the job type is not 'user'.
    """
    options_line = []
    options_line.append("\n\n#////////////////////////////////////////////")
    options_line.append("# Dynamically generated options in a gaudirun job\n")
    if application_name.value == ApplicationName.DaVinci:
        options_line.append("from Gaudi.Configuration import *")
    else:
        options_line.append(f"from {application_name.value}.Configuration import *")

    if extra_options:
        options_line += extra_options

    if input_data_options:
        options_line += input_data_options

    if (
        application_name.value == ApplicationName.Gauss
        and run_number
        and first_event_number
    ):
        options_line.append('GaussGen = GenInit("GaussGen")')
        options_line.append(f"GaussGen.RunNumber = {run_number}")
        options_line.append(f"GaussGen.FirstEventNumber = {first_event_number}")

    if number_of_events != 0:
        options_line.append("ApplicationMgr().EvtMax = %d" % (number_of_events))

    return "\n".join(options_line) + "\n"


# ------------------------------------------------------------------------------


def build_prodconf_json(
    configuration: Configuration,
    output_file_prefix: str,
    computed_run_number: int | None = None,
    first_event_number: int | None = None,
) -> str:
    """Build the prodconf.json file"""
    # application
    application = JobSpecV1.ReleaseApplication(
        name=configuration.application.name,
        version=configuration.application.version,
        event_timeout=configuration.application.event_timeout,
        number_of_processors=configuration.application.number_of_processors,
        data_pkgs=configuration.application.extra_packages,
        binary_tag=configuration.application.system_config,
        nightly=configuration.application.nightly,
    )

    # inputs
    files = configuration.input.files
    tck = configuration.input.tck
    run_number = configuration.input.run_number
    inputs = JobSpecV1.Input(
        files=files if files else None,
        xml_summary_file=f"summary_{output_file_prefix}.xml",
        xml_file_catalog=configuration.input.pool_xml_catalog,
        run_number=run_number
        if run_number and run_number not in ("Unknown", "Multiple")
        else computed_run_number,
        tck=tck if tck else configuration.input.mc_tck,
        n_of_events=configuration.input.number_of_events,
        first_event_number=first_event_number,
    )

    # outputs
    outputs = JobSpecV1.Output(
        prefix=output_file_prefix,
        types=configuration.output.types,
        histogram_file=f"{output_file_prefix}.Hist.root"
        if configuration.output.histogram
        else None,
    )

    # options
    command_options = configuration.options.options
    if isinstance(command_options, dict):
        # This is an lbexec style application
        options = JobSpecV1.LbExecOptions(
            entrypoint=command_options.get("entrypoint"),
            extra_options=command_options.get("extra_options"),
            extra_args=command_options.get("extra_args"),
        )
    else:
        # This is a legacy style application
        options = JobSpecV1.LegacyOptions(
            files=command_options,
            format=configuration.options.format,
            gaudi_extra_options=configuration.options.gaudi_extra_options,
            processing_pass=configuration.options.processing_pass,
        )

    # db_tags
    dddb_tag = configuration.db_tags.dddb_tag
    conddb_tag = configuration.db_tags.conddb_tag
    db_tags = JobSpecV1.DBTags(
        dddb_tag=configuration.db_tags.online_ddb_tag
        if dddb_tag and dddb_tag.lower() == "online"
        else dddb_tag,
        conddb_tag=configuration.db_tags.online_conddb_tag
        if conddb_tag and conddb_tag.lower() == "online"
        else conddb_tag,
        dq_tag=configuration.db_tags.dq_tag,
    )

    # Initialise the prodInfo object
    prod_info = JobSpecV1(
        application=application,
        input=inputs,
        output=outputs,
        db_tags=db_tags,
        options=options,
    )
    prod_info_dict = prod_info.model_dump()
    prod_info_dict["spec_version"] = 1
    logging.info(f"prodConf content: {prod_info_dict}")

    # Write the prodconf.json file
    prodconf_file = f"prodConf_{output_file_prefix}.json"
    with open(prodconf_file, "w") as fp:
        json.dump(prod_info_dict, fp, indent=2)
    return prodconf_file


# ------------------------------------------------------------------------------


async def run_lbprodrun(
    application_name: ApplicationName,
    output_file_prefix: str,
    prodconf_file: str,
    use_prmon: bool = False,
):
    """Run the application using lb-prod-run"""
    application_log = f"{output_file_prefix}.log"
    command = ["lb-prod-run", prodconf_file, "--prmon", "--verbose"]

    if application_name.value == ApplicationName.Gauss and use_prmon:
        prmonPath = "/cvmfs/lhcb.cern.ch/lib/experimental/prmon/bin/prmon"

        command = [
            prmonPath,
            "--json-summary",
            "./prmon_Gauss.json",
            "--",
        ] + command
    logging.info(f"Running command {shlex.join(command)}")

    stdout = ""
    stderr = ""

    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout_fh = open(application_log, "a")
    stderr_fh = stdout_fh

    try:
        await asyncio.gather(
            handle_output(proc.stdout, stdout_fh),
            handle_output(proc.stderr, stderr_fh),
            proc.wait(),
        )
    finally:
        if stdout_fh:
            stdout_fh.close()
        if stderr_fh and stdout_fh != stderr_fh:
            stderr_fh.close()
    returncode = proc.returncode
    return (returncode, stdout, stderr)


async def handle_output(stream, fh):
    """Process the output of a current local execution"""
    while line := await stream.readline():
        line = line.decode(errors="backslashreplace")
        handle_line(line)
        if fh:
            fh.write(line)


def handle_line(line):
    """Print a given line to the standard output if related to an event"""
    if "INFO Evt" in line or "Reading Event record" in line or "lb-run" in line:
        # These ones will appear in the std.out log too
        print(line.rstrip())


if __name__ == "__main__":
    app()

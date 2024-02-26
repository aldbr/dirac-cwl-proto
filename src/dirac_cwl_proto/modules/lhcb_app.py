#!/usr/bin/env python3
import asyncio
import json
import logging
import re
import shlex
import subprocess
from enum import Enum
from typing import Optional

import typer
from LbProdRun.models import JobSpecV1
from rich.console import Console


class ApplicationName(str, Enum):
    Boole = "Boole"
    DaVinci = "DaVinci"
    Gauss = "Gauss"
    LHCb = "LHCb"  # Merge
    Moore = "Moore"


app = typer.Typer()
console = Console()


@app.command()
def run_application(
    # Application
    application_name: ApplicationName = typer.Argument(
        ..., help="Application to execute"
    ),
    application_version: str = typer.Argument(..., help="Application version"),
    extra_packages: Optional[list[str]] = typer.Option(None, help="Extra packages"),
    system_config: Optional[str] = typer.Option(None, help="System configuration"),
    event_timeout: int = typer.Option(3600, help="Event timeout"),
    number_of_processors: int = typer.Option(1, help="Number of processors"),
    nightly: Optional[str] = typer.Option(None, help="Nightly"),
    # Options
    options: str = typer.Argument(..., help="Options"),
    options_format: Optional[str] = typer.Option(None, help="Options format"),
    gaudi_options: Optional[list[str]] = typer.Option(None, help="Gaudi options"),
    gaudi_extra_options: str = typer.Option(None, help="Gaudi extra options"),
    processing_pass: Optional[str] = typer.Option(None, help="Processing pass"),
    # Inputs
    pool_xml_catalog_name: Optional[str] = typer.Option(
        None, help="Pool XML catalog name"
    ),
    run_id: int = typer.Argument(..., help="Run ID"),  # Corresponds to production_id
    task_id: int = typer.Argument(..., help="Task ID"),  # Corresponds to prod_job_id
    inputs: Optional[list[str]] = typer.Option(None, help="Input data"),
    tck: Optional[str] = typer.Option(None, help="TCK"),
    mc_tck: Optional[str] = typer.Option(None, help="MC TCK"),
    run_number: Optional[int] = typer.Option(None, help="Run number"),
    # Outputs
    output_types: list[str] = typer.Argument(..., help="Output types"),
    histogram: bool = typer.Option(False, help="Histogram"),
    # DB Tags
    dddb_tag: Optional[str] = typer.Option(None, help="DDDB tag"),
    online_ddb_tag: Optional[str] = typer.Option(None, help="Online DDB tag"),
    conddb_tag: Optional[str] = typer.Option(None, help="CondDB tag"),
    online_conddb_tag: Optional[str] = typer.Option(None, help="Online CondDB tag"),
    dq_tag: Optional[str] = typer.Option(None, help="DQ tag"),
    # Gauss specific options
    number_of_events: int = typer.Option(-1, help="Number of events"),
    use_prmon: bool = typer.Option(False, help="Use prmon"),
    # Step ID
    step_id: int = typer.Option(0, help="Step ID"),
):
    """
    LHCb application: generates a prodconf.json file and runs the application.
    """
    if application_name.value == ApplicationName.Gauss and not (
        run_id and task_id and number_of_events
    ):
        raise typer.Exit(
            "Simulation ID, Task ID and Number of Events are required for Gauss application"
        )
    if application_name.value != ApplicationName.Gauss and not inputs:
        raise typer.Exit("No MC, but no input data")
    if tck and mc_tck:
        raise typer.Exit("TCK set, but shouldn't be!")
    if (
        dddb_tag != "online"
        and online_ddb_tag
        or conddb_tag != "online"
        and online_conddb_tag
    ):
        raise typer.Exit("DDDB tag set, but shouldn't be!")
    if application_name.value != ApplicationName.Gauss and not pool_xml_catalog_name:
        raise typer.Exit("Pool XML catalog name is required")

    console.print("[bold blue]Getting parameters...[/bold blue]")

    # Get command options
    command_options = get_command_options(options=options)
    console.print(f"[bold blue]Command options: {command_options}[/bold blue]")

    # Get run & event number for Gauss application
    run_number_gauss, first_event_number_gauss = get_run_event_numbers(
        application_name,
        run_id,
        task_id,
        number_of_events,
    )
    if run_number_gauss and first_event_number_gauss:
        console.print(
            f"[bold blue]Run number: {run_number_gauss} & Event number: {first_event_number_gauss}[/bold blue]"
        )

    if not pool_xml_catalog_name:
        pool_xml_catalog_name = "pool_xml_catalog.xml"

    if gaudi_options:
        console.print("[bold blue]Building Gaudi extra options...[/bold blue]")
        # Prepare standard project run time options
        generated_options = build_gaudi_extra_options(
            gaudi_options=gaudi_options,
            application_name=application_name,
            number_of_events=number_of_events,
            pool_xml_catalog_name=pool_xml_catalog_name,
            run_number=run_number_gauss,
            first_event_number=first_event_number_gauss,
            inputs=inputs,
        )
        # If not lbexec style
        if isinstance(command_options, list):
            command_options.append(generated_options)
        console.print("[bold blue]Gaudi extra options generated![/bold blue]")

    output_file_prefix = f"{application_name}_{run_id}_{task_id}_{step_id}"

    console.print("[bold blue]Building prodconf.json file...[/bold blue]")
    prodconf_file = build_prodconf_json(
        application_name=application_name,
        application_version=application_version,
        system_config=system_config,
        pool_xml_catalog_name=pool_xml_catalog_name,
        event_timeout=event_timeout,
        output_file_prefix=output_file_prefix,
        output_types=output_types,
        number_of_processors=number_of_processors,
        run_number_gauss=run_number_gauss,
        first_event_number_gauss=first_event_number_gauss,
        nightly=nightly,
        inputs=inputs,
        extra_packages=extra_packages,
        command_options=command_options,
        options_format=options_format,
        gaudi_extra_options=gaudi_extra_options,
        processing_pass=processing_pass,
        run_number=run_number,
        tck=tck,
        mc_tck=mc_tck,
        dddb_tag=dddb_tag,
        online_ddb_tag=online_ddb_tag,
        conddb_tag=conddb_tag,
        online_conddb_tag=online_conddb_tag,
        dq_tag=dq_tag,
        number_of_events=number_of_events,
        histogram=histogram,
    )
    console.print("[bold blue]prodconf.json file generated...[/bold blue]")

    console.print("[bold blue]Running lb-prod-run prodconf.json...[/bold blue]")
    try:
        status_code = run_lbprodrun(
            application_name=application_name,
            output_file_prefix=output_file_prefix,
            prodconf_file=prodconf_file,
            use_prmon=use_prmon,
        )
    except RuntimeError as e:
        console.print(
            f"[bold red]{application_name} {application_version} Error: {e}[/bold red]"
        )
        raise typer.Exit(code=1) from e

    if status_code != 0:
        raise typer.Exit(
            f"{application_name} {application_version} failed with status {status_code}"
        )

    console.print(
        f"[bold green]lb-prod-run finished with status {status_code}[/bold green]"
    )


# ------------------------------------------------------------------------------


def get_command_options(options: str) -> dict | list[str]:
    """
    Get the command options from the input string
    """
    command_options = []
    # Resolve options files
    if options.startswith("{"):
        command_options = json.loads(options)
        logging.info(f"Found lbexec style configuration: {command_options}")
    else:
        if options and options != "None":
            command_options = options.split(";")
    return command_options


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
    pool_xml_catalog_name: str,
    run_number: int | None,
    first_event_number: int | None,
    inputs: list[str] | None,
) -> str:
    """
    Build the Gaudi extra options
    """
    input_data_options = get_data_options(inputs, pool_xml_catalog_name)
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


def get_data_options(inputs: list[str] | None, pool_xml_catalog_name: str) -> list[str]:
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
        f"""\nFileCatalog().Catalogs= ["xmlcatalog_file:{pool_xml_catalog_name}"]\n"""
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
    application_name: ApplicationName,
    application_version: str,
    pool_xml_catalog_name: str,
    event_timeout: int,
    output_file_prefix: str,
    number_of_processors: int,
    run_number_gauss: int,
    first_event_number_gauss: int,
    output_types: list[str],
    system_config: str | None,
    nightly: str | None,
    inputs: list[str] | None,
    extra_packages: list[str] | None,
    command_options: dict | list[str],
    options_format: str | None,
    gaudi_extra_options: str | None,
    processing_pass: str | None,
    run_number: int | None,
    tck: str | None,
    mc_tck: str | None,
    dddb_tag: str | None,
    online_ddb_tag: str | None,
    conddb_tag: str | None,
    online_conddb_tag: str | None,
    dq_tag: str | None,
    number_of_events: int | None,
    histogram: bool = False,
) -> str:
    """Build the prodconf.json file"""
    histo_name = f"{output_file_prefix}.Hist.root"

    # application
    application = JobSpecV1.ReleaseApplication(
        name=application_name,
        version=application_version,
        event_timeout=event_timeout,
        number_of_processors=number_of_processors,
        data_pkgs=extra_packages if extra_packages else [],
        binary_tag=system_config
        if system_config and system_config.lower() != "any"
        else "best",
        nightly=nightly if nightly else None,
    )

    # inputs
    inputs = JobSpecV1.Input(
        files=[f"LFN:{sid}" for sid in inputs] if inputs else None,
        xml_summary_file=f"summary_{output_file_prefix}.xml",
        xml_file_catalog=pool_xml_catalog_name,
        run_number=run_number
        if run_number and run_number not in ("Unknown", "Multiple")
        else run_number_gauss,
        tck=tck if tck else mc_tck,
        n_of_events=number_of_events,
        first_event_number=first_event_number_gauss,
    )

    # outputs
    outputs = JobSpecV1.Output(
        prefix=output_file_prefix,
        types=output_types,
        histogram_file=histo_name if histogram else None,
    )

    # options
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
            format=options_format,
            gaudi_extra_options=gaudi_extra_options,
            processing_pass=processing_pass,
        )

    # db_tags
    db_tags = JobSpecV1.DBTags(
        dddb_tag=online_ddb_tag
        if dddb_tag and dddb_tag.lower() == "online"
        else dddb_tag,
        conddb_tag=online_conddb_tag
        if conddb_tag and conddb_tag.lower() == "online"
        else conddb_tag,
        dq_tag=dq_tag,
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

    # Write the prodconf.json file
    prodconf_file = f"prodConf_{output_file_prefix}.json"
    with open(prodconf_file, "w") as fp:
        json.dump(prod_info_dict, fp, indent=2)
    return prodconf_file


# ------------------------------------------------------------------------------


def run_lbprodrun(
    application_name: ApplicationName,
    output_file_prefix: str,
    prodconf_file: str,
    use_prmon: bool = False,
):
    """Invokes lb-prod-run (what you call after having setup the object)"""
    returncode, stdout, stderr = asyncio.get_event_loop().run_until_complete(
        run_app(
            application_name=application_name,
            output_file_prefix=output_file_prefix,
            prodconf_file=prodconf_file,
            use_prmon=use_prmon,
        )
    )
    if returncode != 0:
        logging.error(f"lb-run or its application exited with status {returncode}")
        logging.error(stderr)
        raise RuntimeError(f"Application exited with status {returncode}")

    return (returncode, stdout, stderr)


async def run_app(
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


# def get_event_timeout(event_timeout: int) -> int:
#     """
#     Get the event timeout
#     """
#     # Simple check for slow processors: auto increase of Event Timeout
#     cpuNormalization = int(gConfig.getValue("/LocalSite/CPUNormalizationFactor", 10))
#     if cpuNormalization < 10:
#         event_timeout = int(event_timeout * 10 / cpuNormalization)
#     return event_timeout

# def get_events_to_produce(
#     cpu_work_per_event,
#     cpu_time=None,
#     cpu_power=None,
#     max_number_of_events=None,
#     max_cpu_time=None,
# ):
#     """Returns the number of events to produce considering the CPU time
#     available. CPU Time and CPU Power are taken from the LocalSite
#     configuration if not provided. No checks are made on the values passed!

#     Limits can be set.
#     """
#     if cpu_power is None:
#         cpu_power = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 1.0)

#     if cpu_time is None:
#         cpu_time = getCPUTime(cpu_power)
#     if max_cpu_time:
#         logging.debug(
#             f"CPU Time left from the WN perspective: {cpu_time}; Job maximum CPU Time (in seconds): {max_cpu_time}"
#         )
#         cpu_time = min(cpu_time, max_cpu_time)

#     logging.debug(
#         "CPU Time = %d, CPU Power = %f, CPU Work per Event = %d"
#         % (cpu_time, cpu_power, cpu_work_per_event)
#     )

#     events_to_produce = int(
#         math.floor(cpu_time * cpu_power) / float(cpu_work_per_event)
#     )

#     logging.info(f"We can produce {events_to_produce} events")
#     events_to_produce = int(events_to_produce * 0.75)
#     logging.info(
#         f"But we take a conservative approach, so 75%% of those: {events_to_produce}"
#     )

#     if max_number_of_events:
#         logging.info(f"Limit for Max Number Of Events: {max_number_of_events}")
#         events_to_produce = min(events_to_produce, max_number_of_events)

#     if events_to_produce < 1:
#         raise RuntimeError("No time left to produce events")

#     return events_to_produce

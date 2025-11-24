#!/usr/bin/env python3
"""Wrapper for lb-prod-run that handles CWL inputs and configuration merging."""
import asyncio
import argparse
import json
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="LbProdRun Wrapper for DIRAC CWL")
    parser.add_argument("config_file", help="Base configuration JSON file")
    parser.add_argument("--input-files", help="Input paths that are resolved from direct local file paths (txt file)")
    parser.add_argument("--pool-xml-catalog", default="pool_xml_catalog.xml", help="Pool XML catalog file")
    parser.add_argument("--run-number", type=int, help="Run number")
    parser.add_argument("--first-event-number", type=int, help="First event number")
    parser.add_argument("--number-of-events", type=int, help="Number of events")
    parser.add_argument("--number-of-processors", type=int, help="Number of processors")
    parser.add_argument("--output-prefix", help="Output file prefix")
    parser.add_argument("--event-type", help="Event type ID for Gauss")
    parser.add_argument("--histogram", action="store_true", help="Enable histogram output")

    args = parser.parse_args()

    # Load base configuration
    config = json.loads(Path(args.config_file).read_text())

    # Merge command-line arguments
    if args.number_of_events is not None:
        config["input"]["n_of_events"] = args.number_of_events

    if args.number_of_processors is not None:
        config["application"]["number_of_processors"] = args.number_of_processors

    if args.output_prefix:
        config["output"]["prefix"] = args.output_prefix

    if args.run_number is not None:
        config["input"]["run_number"] = args.run_number

    if args.first_event_number is not None:
        config["input"]["first_event_number"] = args.first_event_number

    if args.histogram:
        config["output"]["histogram"] = True

    if args.pool_xml_catalog:
        config["input"]["xml_file_catalog"] = Path(args.pool_xml_catalog).name

    if args.input_files:
        paths = Path(args.input_files).read_text().splitlines()
        config["input"]["files"] = [f"PFN:{path.strip()}" for path in paths]
    
    # check the options files for @{eventType} if application is Gauss
    if config["application"]["name"].lower() == "gauss":
        options = config["options"].get("files", [])
        if isinstance(options, list):
            if not [opt for opt in options if "@{eventType}" in opt]:
                raise ValueError("For Gauss, at least one option file path must contain the '@{eventType}' placeholder.")
        if args.event_type is None:
            raise ValueError("Event type ID must be provided for Gauss application.")
        # substitute event type in options
        config["options"]["files"] = [opt.replace("@{eventType}", args.event_type) for opt in options]
        
    app_name = config["application"]["name"]
    cleaned_appname = app_name.replace("/", "").replace(" ", "")

    config_filename = f"prodConf_{cleaned_appname}_{args.output_prefix}.json"
    output_config = Path(config_filename)
    output_config.write_text(json.dumps(config, indent=2))

    # Run lb-prod-run with the merged configuration
    returncode, _, _ = asyncio.run(
        run_lbprodrun(
            application_log=f"{cleaned_appname}_{args.output_prefix}.log",
            prodconf_file=config_filename,
        )
    )
    sys.exit(returncode)


async def run_lbprodrun(
    application_log: str,
    prodconf_file: str,
):
    """Run the application using lb-prod-run"""
    command = ["lb-prod-run", prodconf_file, "--prmon", "--verbose"]

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


async def readlines(stream: asyncio.StreamReader, chunk_size: int = 4096, errors: str = "backslashreplace"):
    """Read lines from a stream"""
    buffer = b""
    while not stream.at_eof():
        chunk = await stream.read(chunk_size)
        if not chunk:
            break
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line.decode(errors=errors)
    if buffer:
        yield buffer.decode(errors=errors)


async def handle_output(stream: asyncio.StreamReader, fh):
    """Process output of lb-prod-run."""
    async for line in readlines(stream):
        if "INFO Evt" in line or "Reading Event record" in line or "lb-run" in line:
            # These ones will appear in the std.out log too
            print(line.rstrip())
        if fh:
            fh.write(line + "\n")


if __name__ == "__main__":
    main()

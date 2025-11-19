#!/usr/bin/env python3
"""Wrapper for lb-prod-run that handles CWL inputs and configuration merging."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Run LbProdRun with CWL inputs")
    parser.add_argument("config_file", help="Base configuration JSON file")
    parser.add_argument("--lfn-paths", help="Input LFN paths (JSON file)")
    parser.add_argument("--pfn-paths", help="Input PFN paths (JSON file)")
    parser.add_argument("--pool-xml-catalog", help="Pool XML catalog file")
    parser.add_argument("--run-number", type=int, help="Run number")
    parser.add_argument("--first-event-number", type=int, help="First event number")
    parser.add_argument("--number-of-events", type=int, help="Number of events")
    parser.add_argument("--number-of-processors", type=int, help="Number of processors")
    parser.add_argument("--output-prefix", help="Output file prefix")
    parser.add_argument("--histogram", action="store_true", help="Enable histogram output")
    parser.add_argument("--output-type", help="Override output type")

    args = parser.parse_args()

    # Load base configuration
    config = json.loads(Path(args.config_file).read_text())

    # Merge command-line arguments
    if args.number_of_events is not None:
        config["input"]["number_of_events"] = args.number_of_events

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

    if args.lfn_paths:
        paths = json.loads(Path(args.lfn_paths).read_text())
        config["input"]["files"] = paths

    if args.pfn_paths:
        paths = json.loads(Path(args.pfn_paths).read_text())
        config["input"]["files"] = paths

    # Write updated configuration
    output_config = Path("runtime_config.json")
    output_config.write_text(json.dumps(config, indent=2))

    # Run lb-prod-run
    result = subprocess.run(["lb-prod-run", str(output_config)])
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()

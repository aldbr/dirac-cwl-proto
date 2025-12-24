"""CLI tool for running CWL workflows with DIRAC executor.

This command-line tool runs CWL workflows using the DiracExecutor, which handles
replica catalog management for input and output files.
"""

import argparse
import sys
from pathlib import Path


def main():
    """Entry point for the command-line tool."""
    parser = argparse.ArgumentParser(
        description="Run CWL workflows with DIRAC executor and replica catalog management",
        prog="dirac-cwl-run",
    )

    # Required arguments
    parser.add_argument("workflow", type=Path, help="Path to CWL workflow file")
    parser.add_argument(
        "inputs", type=Path, nargs="?", help="Path to inputs YAML file (optional)"
    )

    # Optional arguments
    parser.add_argument(
        "--outdir", type=Path, help="Output directory (default: current directory)"
    )
    parser.add_argument("--tmpdir-prefix", type=Path, help="Temporary directory prefix")
    parser.add_argument(
        "--leave-tmpdir", action="store_true", help="Keep temporary directories"
    )
    parser.add_argument(
        "--replica-catalog", type=Path, help="Path to master replica catalog JSON file"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--parallel", action="store_true", help="Run jobs in parallel")
    parser.add_argument(
        "--version", action="store_true", help="Show version information"
    )

    args = parser.parse_args()

    if args.version:
        print("DIRAC CWL Executor")
        print("Version: 0.1.0")
        print("Built with cwltool and DIRAC integration")
        return 0

    # Build cwltool arguments
    cwltool_args = [
        "--outdir",
        str(args.outdir) if args.outdir else ".",
        "--disable-color",  # Disable ANSI color codes in logs
    ]

    if args.tmpdir_prefix:
        cwltool_args.extend(["--tmpdir-prefix", str(args.tmpdir_prefix)])

    if args.leave_tmpdir:
        cwltool_args.append("--leave-tmpdir")

    if args.debug:
        cwltool_args.append("--debug")
    elif args.verbose:
        cwltool_args.append("--verbose")

    if args.parallel:
        cwltool_args.append("--parallel")

    # Add workflow and inputs
    cwltool_args.append(str(args.workflow))
    if args.inputs:
        cwltool_args.append(str(args.inputs))

    try:
        # Create our custom DIRAC executor with replica catalog support
        from cwltool.main import main as cwltool_main

        from dirac_cwl_proto.executor import DiracExecutor

        dirac_executor = DiracExecutor(master_catalog_path=args.replica_catalog)

        print("=" * 80)
        print("DIRAC CWL Workflow Executor")
        print("=" * 80)
        print(f"Workflow: {args.workflow}")
        if args.inputs:
            print(f"Inputs: {args.inputs}")
        print(f"Output directory: {args.outdir if args.outdir else '.'}")
        print()
        print("✓ Using DIRAC executor with replica catalog management")
        if args.replica_catalog:
            print(f"✓ Master catalog: {args.replica_catalog}")
        else:
            print("⚠ No replica catalog provided - will create empty catalog")
        print()

        # Run cwltool with our custom executor
        # We pass a NullHandler to suppress cwltool's default logging
        # and let it set up its own handler that does nothing
        import logging

        null_handler = logging.NullHandler()

        exit_code = cwltool_main(
            argsl=cwltool_args,
            executor=dirac_executor,
            logger_handler=null_handler,
        )

        if exit_code == 0:
            print("\n✅ Workflow executed successfully!")
            print(f"   Outputs available in: {args.outdir if args.outdir else '.'}")

            # Show catalog stats if available
            if args.replica_catalog and args.replica_catalog.exists():
                print(f"   Replica catalog: {args.replica_catalog}")
                try:
                    from dirac_cwl_proto.job.replica_catalog import ReplicaCatalog

                    catalog = ReplicaCatalog.model_validate_json(
                        args.replica_catalog.read_text()
                    )
                    print(f"   Catalog entries: {len(catalog.root)}")
                except Exception:
                    pass
        else:
            print(f"\n❌ Workflow execution failed with exit code {exit_code}")

        return exit_code

    except Exception as e:
        import traceback

        traceback.print_exc()
        print(f"\n❌ Error executing workflow: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

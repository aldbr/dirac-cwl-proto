#!/usr/bin/env python

import json
import logging
import sys
import tempfile

from cwl_utils.parser import load_document_by_uri
from ruamel.yaml import YAML

from dirac_cwl_proto.job.JobWrapper import JobWrapper
from dirac_cwl_proto.submission_models import JobSubmissionModel


def main():
    """Execute the job wrapper for a given job model."""

    if len(sys.argv) != 2:
        logging.error("1 argument is required")
        sys.exit(1)

    job_json_file = sys.argv[1]
    job_wrapper = JobWrapper()
    with open(job_json_file, "r") as file:
        job_model_dict = json.load(file)

    task_dict = job_model_dict["task"]

    with tempfile.NamedTemporaryFile("w+", suffix=".cwl", delete=False) as f:
        YAML().dump(task_dict, f)
        f.flush()
        task_obj = load_document_by_uri(f.name)

    job_model_dict["task"] = task_obj

    job = JobSubmissionModel.model_validate(job_model_dict)

    res = job_wrapper.run_job(job)
    if res:
        logging.info("Job done.")
    else:
        logging.info("Job failed.")


if __name__ == "__main__":
    main()

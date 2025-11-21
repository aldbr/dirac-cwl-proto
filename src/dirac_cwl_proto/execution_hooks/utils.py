from importlib.metadata import entry_points


def obtain_job_processor(job_type: str):
    """
    raises: KeyError if
    """
    jobTypeProcessor = None

    if job_type is not None:
        jobTypeMap = entry_points(group="modules.jobTypes")
        jobTypeClass = jobTypeMap[job_type].load()
        jobTypeProcessor = jobTypeClass()

    return jobTypeProcessor

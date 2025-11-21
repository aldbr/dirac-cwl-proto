from importlib.metadata import entry_points

from dirac_cwl_proto.commands import JobTypeProcessorBase


def obtain_job_processor(job_type: str) -> JobTypeProcessorBase:
    """Helper function that reads the entry-poins and returns a JobTypeProcessor depending on the job_type parameter.

    Args:
        job_type (str): Name of the Job Type that should be in the entry-points map

    Returns:
        JobTypeProcessorBase: Subclass of JobTypeProcessorBase specified at the entry-points map

    Raises:
        KeyError: If the job_type is not at the group "modules.jobTypes" in the entry-points map.

        ValueError: If the loaded class does not inherit JobTypeProcessorBase
    """
    job_type_map = entry_points(group="modules.jobTypes")
    job_type_class = job_type_map[job_type].load()
    job_type_processor = job_type_class()

    if not isinstance(job_type_processor, JobTypeProcessorBase):
        raise ValueError(
            f"Class configured in entry-points map of job type '{job_type}'"
            f"is not a subclass of '{JobTypeProcessorBase.__name__}'"
        )

    return job_type_processor

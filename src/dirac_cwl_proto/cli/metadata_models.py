from pathlib import Path

from pydantic import BaseModel


class IMetadataModel(BaseModel):
    """Metadata for a transformation."""

    def get_bk_path(self) -> Path:
        """
        Template method for getting the output path to store results of a job/get results of a previous job.
        Should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class BasicMetadataModel(IMetadataModel):
    """Very basic metadata model."""

    max_random: int
    min_random: int
    input_data: list[dict[str, str]] | None = None

    def get_bk_path(self) -> Path:
        """Get the bk path."""
        # Create the "bookkeeping" path
        output_path = Path("bookkeeping") / str(self.max_random) / str(self.min_random)
        output_path.mkdir(exist_ok=True, parents=True)
        return output_path


class MacobacMetadataModel(IMetadataModel):
    """Very basic metadata model."""

    configuration: Path
    input_data: list[dict[str, str]] | None = None

    def get_bk_path(self) -> Path:
        """Get the bk path."""
        return Path("bookkeeping")


class LHCbMetadataModel(IMetadataModel):
    """LHCb metadata model version 1."""

    task_id: int
    run_id: int

    def get_bk_path(self) -> Path:
        """Get the bk path."""
        # Create the "bookkeeping" path
        output_path = Path("bookkeeping") / str(self.task_id) / str(self.run_id)
        output_path.mkdir(exist_ok=True, parents=True)
        return output_path

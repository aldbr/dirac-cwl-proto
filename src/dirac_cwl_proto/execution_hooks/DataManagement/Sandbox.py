import logging
import random
import tarfile
from pathlib import Path
from typing import Optional, Sequence

logger = logging.getLogger(__name__)


class SandboxStoreClient:
    """Class used for sandbox operation"""

    def uploadFilesAsSandbox(
        self,
        fileList: Sequence[Path | str],
        sizeLimit: int = 0,
        assignTo: Optional[dict] = None,
    ) -> Optional[Path]:
        """Upload files to the sandbox store

        Parameters
        ----------
        fileList : Sequence[Path | str]
            Files to be stored.

        assignTo : Optional[Dict]
            containing { 'Job:<jobid>' : '<sbType>', ... }
        Returns
        -------
        Optional[Path]
            The path of the new sandbox.
        """
        if len(fileList) == 0:
            return None
        Path("sandboxstore").mkdir(exist_ok=True)
        sandbox_id = random.randint(1000, 9999)
        sandbox_path = Path("sandboxstore") / f"sandbox_{str(sandbox_id)}.tar.gz"
        if not sandbox_path:
            raise RuntimeError(f"No output sanbox path defined for {fileList}")
        sandbox_path.parent.mkdir(exist_ok=True, parents=True)
        with tarfile.open(sandbox_path, "w:gz") as tar:
            for file in fileList:
                if not file:
                    break
                if isinstance(file, str):
                    file = Path(file)
                tar.add(file, arcname=file.name)
        return sandbox_path

    def downloadSandbox(
        self,
        sbLocation: str | Path,
        destinationDir: str = "",
        inMemory: bool = False,
        unpack: bool = True,
    ) -> list[Path]:
        # Download the files from the sandbox store
        if not unpack:
            raise NotImplementedError
        else:
            sandbox_path = Path(sbLocation)
            with tarfile.open(sandbox_path, "r:gz") as tar:
                tar.extractall(destinationDir, filter="data")
                files = tar.getnames()
            logger.info("Files downloaded successfully!")
            return [Path(destinationDir) / file for file in files]

    def downloadSandboxForJob(
        self, jobId, sbType, destinationPath="", inMemory=False, unpack=True
    ) -> None:
        raise NotImplementedError

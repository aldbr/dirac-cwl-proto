import logging
import random
import tarfile
from pathlib import Path
from typing import Optional, Sequence

from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient  # type: ignore[import-untyped]
from DIRACCommon.Core.Utilities.ReturnValues import S_OK  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class MockSandboxStoreClient(SandboxStoreClient):
    def __init__(self):
        pass

    def uploadFilesAsSandbox(
        self,
        fileList: Sequence[Path | str],
        sizeLimit: int = 0,
        assignTo: Optional[dict] = None,
    ):
        """Create and upload a sandbox archive from a list of files.

        Packages the provided files into a compressed tar archive and stores
        it under the local sandbox directory.

        Parameters
        ----------
        fileList : Sequence[Path | str]
            Files to be included in the sandbox.
        sizeLimit : int, optional
            Maximum allowed archive size in bytes. Currently unused.
        assignTo : Optional[dict], optional
            Mapping of job identifiers to sandbox types (e.g. { 'Job:<id>': '<type>' }).

        Returns
        -------
        Optional[Path]
            Path to the created sandbox archive, or ``None`` if no files were provided.
        """
        if len(fileList) == 0:
            return None
        sandbox_id = random.randint(1000, 9999)
        sandbox_path = Path("sandboxstore") / f"sandbox_{str(sandbox_id)}.tar.gz"
        sandbox_path.parent.mkdir(exist_ok=True, parents=True)
        with tarfile.open(sandbox_path, "w:gz") as tar:
            for file in fileList:
                if not file:
                    break
                if isinstance(file, str):
                    file = Path(file)
                tar.add(file, arcname=file.name)
        res = S_OK(str(sandbox_path))
        res["SandboxFileName"] = f"sandbox_{str(sandbox_id)}.tar.gz"
        return res

    def downloadSandbox(
        self,
        sbLocation: str | Path,
        destinationDir: str = "",
        inMemory: bool = False,
        unpack: bool = True,
    ) -> list[Path]:
        """Download and extract files from a sandbox archive.

        Opens the given sandbox archive and extracts its contents to the specified
        directory.

        Parameters
        ----------
        sbLocation : str | Path
            Path to the sandbox archive file.
        destinationDir : str, optional
            Directory to extract the files into. Defaults to the current directory.
        inMemory : bool, optional
            Placeholder for in-memory extraction. Currently unused.
        unpack : bool, optional
            Whether to unpack the archive. Only unpacking is currently supported.

        Returns
        -------
        list[Path]
            List of paths to the extracted files.

        Raises
        ------
        NotImplementedError
            If unpacking is disabled.
        """
        if not unpack or inMemory:
            raise NotImplementedError
        else:
            sandbox_path = Path("sandboxstore") / f"{sbLocation}.tar.gz"
            with tarfile.open(sandbox_path, "r:gz") as tar:
                tar.extractall(destinationDir, filter="data")
                files = tar.getnames()
            logger.info("Files downloaded successfully!")
            return S_OK([str(Path(destinationDir) / file) for file in files])

    def downloadSandboxForJob(
        self, jobId, sbType, destinationPath="", inMemory=False, unpack=True
    ) -> None:
        """
        Download sandbox contents for a specific job.

        Placeholder for future implementation of job-based sandbox retrieval.
        """
        raise NotImplementedError

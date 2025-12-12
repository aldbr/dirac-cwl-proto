import json
import time
from pathlib import Path

from DIRAC import S_ERROR, S_OK  # type: ignore[import-untyped]
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog  # type: ignore[import-untyped]


class InMemoryFileCatalog(FileCatalog):
    """Minimal in-memory FileCatalog compatible with DIRAC DataManager."""

    def __init__(self, catalogs=None, vo=None):
        self._eligibleCatalogs = {}
        self._files = {}  # store metadata and logical file names
        super(FileCatalog, self).__init__()

    def _getEligibleCatalogs(self):
        self._eligibleCatalogs = {"MyMockCatalog": {"Type": "MockFileCatalog", "Backend": "Memory"}}
        return S_OK(self._eligibleCatalogs)

    def findFile(self, lfn):
        if lfn in self._files:
            return S_OK([self._files[lfn]])
        return S_ERROR(f"File {lfn} not found")

    def addFile(self, lfn, metadata=None):
        if lfn in self._files:
            return S_ERROR(f"File {lfn} already exists")
        self._files[lfn] = {"LFN": lfn, "Metadata": metadata or {}}
        return S_OK(lfn)


class LocalFileCatalog(FileCatalog):
    def __init__(self, catalogs=None, vo=None):
        self._eligibleCatalogs = {"MyMockCatalog": {"Type": "MockFileCatalog", "Backend": "LocalFileSystem"}}
        self._metadataPath = "filecatalog/metadata.json"
        super(FileCatalog, self).__init__()

    def _getEligibleCatalogs(self):
        return S_OK(self._eligibleCatalogs)

    def getFileMetadata(self, lfn):
        metaAll = self._getAllMetadata()
        if lfn not in metaAll:
            return S_OK({"Successful": {}, "Failed": {lfn: f"File {lfn} not found"}})
        return S_OK({"Successful": {lfn: metaAll[lfn]}, "Failed": {}})

    def addFile(self, lfn):
        if lfn in self._getAllMetadata():
            return S_ERROR(f"File {lfn} already exists")
        self.setMetadata(lfn, {"CreationDate": time.time()})
        return S_OK({"Successful": {lfn: True}, "Failed": {}})

    def setMetadata(self, lfn, metadataDict):
        meta = self._getAllMetadata()
        meta[lfn] = metadataDict

        try:
            self._setAllMetadata(meta)
        except Exception as e:
            return S_ERROR(f"Could set metadata: {e}")
        return S_OK({"Successful": {lfn: True}, "Failed": {}})

    def _getAllMetadata(self):
        try:
            with open(self._metadataPath, "r") as file:
                meta = json.load(file)
        except Exception:
            meta = {}
        return meta

    def _setAllMetadata(self, metadata):
        Path(self._metadataPath).parent.mkdir(parents=True, exist_ok=True)
        with open(self._metadataPath, "w+") as file:
            json.dump(metadata, file)

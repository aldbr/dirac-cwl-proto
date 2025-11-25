from DIRAC import S_ERROR, S_OK  # type: ignore[import-untyped]
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog  # type: ignore[import-untyped]


# ---------------------------
# In-memory FileCatalog
# ---------------------------
class InMemoryFileCatalog(FileCatalog):
    """Minimal in-memory FileCatalog compatible with DIRAC DataManager."""

    def __init__(self, catalogs=None, vo=None):
        self._eligibleCatalogs = {}
        self._files = {}  # store metadata and logical file names
        super(FileCatalog, self).__init__()

    def _getEligibleCatalogs(self):
        self._eligibleCatalogs = {
            "MyMockCatalog": {"Type": "MockFileCatalog", "Backend": "Memory"}
        }
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

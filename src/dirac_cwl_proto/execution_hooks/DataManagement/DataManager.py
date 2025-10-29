from pathlib import Path

from DIRAC.Resources.Storage.FileStorage import FileStorage  # type: ignore[import-untyped]

from dirac_cwl_proto.execution_hooks.DataManagement.FileCatalog import LocalFileCatalog


class DataManager:
    """
    Interface for the DIRAC DataManager

    Used for data operations and should delegates to both Storage Elements and File Catalogs
    """

    def __init__(self, file_catalog=None):
        if file_catalog == "LocalFileCatalog":
            self.file_catalog = LocalFileCatalog()
            self.base_storage_path = "filecatalog"
            self.storage_element = FileStorage(
                "local", {"Path": self.base_storage_path}
            )
        else:
            self.file_catalog = None

    def getReplicasFromDirectory(self, directory):
        pass

    def getFilesFromDirectory(
        self,
        directory,
    ):
        pass

    def getFile(self, lfn, destinationDir=".", sourceSE=None):
        if isinstance(lfn, list):
            lfns = lfn
        elif isinstance(lfn, str):
            lfns = [lfn]
        else:
            raise ValueError()

        replicas = self.getReplicas(lfns)
        if replicas:
            if not sourceSE:
                sourceSE = self.storage_element
            for lfn in lfns:
                res = sourceSE.getFile(lfn, destinationDir)
                if not res["OK"]:
                    raise FileNotFoundError("Could not download lfn")
                return res["Value"]["Successful"]

    def putAndRegister(
        self,
        lfn,
        fileName,
        diracSE=None,
        guid=None,
        path=None,
        checksum=None,
        overwrite=None,
    ):
        # TODO use file catalog as well
        return self.put(lfn, fileName, diracSE, path)

    def removeFile(self, lfn, force=None):
        pass

    def removeReplica(self, storageElementName, lfn):
        pass

    def put(self, lfn, fileName, diracSE=None, path=None):
        se = diracSE if diracSE else self.storage_element
        if not se:
            raise RuntimeError("No Storage Element defined")
        if not path:
            path = str(lfn).removeprefix("lfn:")
        dest = str(Path(path) / Path(fileName).name)
        se.putFile({dest: fileName})

    def getReplicas(self, lfns):
        if self.file_catalog:
            return self.file_catalog.getReplicas(lfns)
        return None

    # TODO: check if other methods are needed

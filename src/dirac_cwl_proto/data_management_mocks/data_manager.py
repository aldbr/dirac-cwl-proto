from pathlib import Path

from DIRAC.DataManagementSystem.Client.DataManager import DataManager  # type: ignore[import-untyped]
from DIRAC.Resources.Storage.FileStorage import FileStorage  # type: ignore[import-untyped]
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK  # type: ignore[import-untyped]


class MockDataManager(DataManager):
    """
    Mock DIRAC DataManager for local file storage
    """

    def __init__(self):
        self.base_storage_path = "filecatalog"
        self.storage_element = FileStorage("local", {"Path": self.base_storage_path})

    def getFile(self, lfn, destinationDir=".", sourceSE=None, diskOnly=False):
        """Get local copy of LFN(s) from Storage Elements.

        :param mixed lfn: a single LFN or list of LFNs.
        :param str destinationDir: directory to which the file(s) will be
        downloaded. (Default: current working directory).
        :param str sourceSE: source SE from which to download (Default: all replicas will be attempted).
        :param bool diskOnly: chooses the disk ONLY replica(s). (Default: False)
        :return: S_OK({"Successful": {}, "Failed": {}})/S_ERROR(errMessage).
        """
        if isinstance(lfn, list):
            lfns = lfn
        elif isinstance(lfn, str):
            lfns = [lfn]
        else:
            return S_ERROR(f"wrong type for lfn: {lfn}, expected str or list[str]")

        if not sourceSE:
            sourceSE = self.storage_element
        for lfn in lfns:
            res = sourceSE.getFile(str(lfn).removeprefix("lfn:"), destinationDir)
            if not res["OK"]:
                return S_ERROR(f"Could not download lfn {lfn} : {res['Message']}")
        return S_OK([Path(lfn).name for lfn in lfns])

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
        """Put a local file to a Storage Element and register in the File Catalogues

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'guid' is the guid with which the file is to be registered (if not provided will be generated)
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
        'overwrite' removes file from the file catalogue and SE before attempting upload
        """

        return self.put(lfn, fileName, diracSE, path)

    def put(self, lfn, fileName, diracSE=None, path=None):
        """Put a local file to a Storage Element

        :param self: self reference
        :param str lfn: LFN
        :param str fileName: the full path to the local file
        :param str diracSE: the Storage Element to which to put the file
        :param str path: the path on the storage where the file will be put (if not provided the LFN will be used)

        """
        se = self.storage_element
        if not se:
            return S_ERROR("No Storage Element defined")
        if not path:
            path = str(lfn).removeprefix("lfn:")
        dest = str(Path(path) / Path(fileName).name)
        res = se.putFile({dest: fileName})
        if not res["OK"]:
            return res
        return S_OK({"Successful": [lfn], "Failed": []})

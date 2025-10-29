import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class FileCatalogInterface(ABC):
    """Abstract interface for file catalog operations."""

    @abstractmethod
    def add_file(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def remove_file(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def exists(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def getFileMetadata(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def findFilesByMetadata(self, metaDict: dict, path: str, **kwargs) -> dict:
        pass

    @abstractmethod
    def listDirectory(self, lfn: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def isFile(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def isDirectory(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def getReplicas(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def addReplica(self, lfns: str | dict, **kwargs) -> dict:
        pass

    @abstractmethod
    def removeReplica(self, lfns: str | dict, **kwargs) -> dict:
        pass


class LocalFileCatalog(FileCatalogInterface):
    def getReplicas(self, lfns, **kwargs):
        return {lfn: {"local": str(lfn).removeprefix("lfn:")} for lfn in lfns}

    # TODO
    def add_file(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def remove_file(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def exists(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def getFileMetadata(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def findFilesByMetadata(self, metaDict: dict, path: str, **kwargs) -> dict:
        raise NotImplementedError

    def listDirectory(self, lfn: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def isFile(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def isDirectory(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def addReplica(self, lfns: str | dict, **kwargs) -> dict:
        raise NotImplementedError

    def removeReplica(self, lfns: str | dict, **kwargs) -> dict:
        raise NotImplementedError

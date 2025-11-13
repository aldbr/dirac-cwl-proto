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
    def get_file_metadata(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def find_files_by_metadata(self, metaDict: dict, path: str, **kwargs) -> dict:
        pass

    @abstractmethod
    def list_directory(self, lfn: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def is_file(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def is_directory(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def get_replicas(self, lfns: str | list, **kwargs) -> dict:
        pass

    @abstractmethod
    def add_replica(self, lfns: str | dict, **kwargs) -> dict:
        pass

    @abstractmethod
    def remove_replica(self, lfns: str | dict, **kwargs) -> dict:
        pass


class LocalFileCatalog(FileCatalogInterface):
    def get_replicas(self, lfns, **kwargs):
        return {lfn: {"local": str(lfn).removeprefix("lfn:")} for lfn in lfns}

    # TODO
    def add_file(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def remove_file(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def exists(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def get_file_metadata(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def find_files_by_metadata(self, metaDict: dict, path: str, **kwargs) -> dict:
        raise NotImplementedError

    def list_directory(self, lfn: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def is_file(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def is_directory(self, lfns: str | list, **kwargs) -> dict:
        raise NotImplementedError

    def add_replica(self, lfns: str | dict, **kwargs) -> dict:
        raise NotImplementedError

    def remove_replica(self, lfns: str | dict, **kwargs) -> dict:
        raise NotImplementedError

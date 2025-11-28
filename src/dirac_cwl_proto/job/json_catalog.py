
from typing import Annotated
from pydantic import BaseModel, RootModel, BeforeValidator, AnyUrl


def validateLFN(value: str) -> str:
    value = value.removeprefix("LFN:")
    return value

def validatePFN(value: str) -> str:
    value = value.removeprefix("PFN:")
    return value

# Logical File Name such as LFN:/lhcb/MC/2024/HLT2.DST/00327923/0000/00327923_00000533_1.hlt2.dst
LFN = Annotated[str, BeforeValidator(validateLFN)]
PFN = Annotated[AnyUrl, BeforeValidator(validatePFN)]
StorageElementId = str
ChecksumType = str
FileChecksum = str


class JSONCatalog(RootModel):
    class CatalogEntry(BaseModel):
        class Replica(BaseModel):
            url: PFN
            se: StorageElementId
        replicas: list[Replica]
        size_bytes: int | None = None
        checksum: dict[ChecksumType, FileChecksum] | None = None

    root: dict[LFN, CatalogEntry]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

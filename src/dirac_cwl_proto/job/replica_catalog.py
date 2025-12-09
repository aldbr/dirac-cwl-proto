import re
from typing import Annotated

from pydantic import (
    AnyUrl,
    BaseModel,
    BeforeValidator,
    FilePath,
    RootModel,
    field_validator,
)


def validateLFN(value: str) -> str:
    """Validate and normalize Logical File Name.

    Removes LFN: prefix if present and ensures it's a valid absolute path.
    """
    value = value.removeprefix("LFN:")
    if not value.startswith("/"):
        raise ValueError(f"LFN must start with '/': {value}")
    if not value:
        raise ValueError("LFN cannot be empty")
    return value


def validatePFN(value: str) -> str:
    """Validate and normalize Physical File Name.

    Removes PFN: prefix if present before URL validation.
    """
    value = value.removeprefix("PFN:")
    if not value:
        raise ValueError("PFN cannot be empty")
    return value


def validateAdler32(value: str) -> str:
    """Validate Adler32 checksum format.

    Must be 8 hexadecimal characters.
    """
    value = value.lower()
    if len(value) != 8:
        raise ValueError(
            f"Adler32 checksum must be 8 characters long, got {len(value)}: {value}"
        )
    if not re.match(r"^[0-9a-f]{8}$", value):
        raise ValueError(
            f"Adler32 checksum must contain only hexadecimal characters: {value}"
        )
    return value


def validateGUID(value: str) -> str:
    """Validate GUID checksum format.

    The format is 8-4-4-4-12 hexadecimal digits with hyphens (UUID format).
    Example: 6032CB7C-32DC-EC11-9A66-D85ED3091D71
    """
    value = value.upper()
    if len(value) != 36:
        raise ValueError(
            f"GUID checksum must be 36 characters long (including hyphens), got {len(value)}: {value}"
        )

    # Validate UUID format: 8-4-4-4-12 with hyphens
    if not re.match(
        r"^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$", value
    ):
        raise ValueError(f"GUID checksum must follow format 8-4-4-4-12 (UUID): {value}")

    return value


# Logical File Name such as LFN:/lhcb/MC/2024/HLT2.DST/00327923/0000/00327923_00000533_1.hlt2.dst
LFN = Annotated[str, BeforeValidator(validateLFN)]
PFN = Annotated[AnyUrl | FilePath, BeforeValidator(validatePFN)]
StorageElementId = str

Adler32Checksum = Annotated[str, BeforeValidator(validateAdler32)]
GUIDChecksum = Annotated[str, BeforeValidator(validateGUID)]


class ReplicaCatalog(RootModel):
    class CatalogEntry(BaseModel):
        class Replica(BaseModel):
            url: PFN
            se: StorageElementId

            @field_validator("se")
            @classmethod
            def validate_se(cls, v: str) -> str:
                if not v or not v.strip():
                    raise ValueError("Storage Element ID cannot be empty")
                return v.strip()

        class Checksum(BaseModel):
            adler32: Adler32Checksum | None = None  # like 788c5caa
            guid: GUIDChecksum | None = (
                None  # Like 6032CB7C-32DC-EC11-9A66-D85ED3091D71
            )

            @field_validator("adler32", "md5", "guid")
            @classmethod
            def validate_at_least_one_checksum(cls, v, info):
                # This validator runs for each field, so we check after all fields are processed
                return v

        replicas: list[Replica]
        size_bytes: int | None = None
        checksum: Checksum | None = None

        @field_validator("replicas")
        @classmethod
        def validate_replicas(cls, v: list) -> list:
            if not v:
                raise ValueError("At least one replica is required")
            return v

        @field_validator("size_bytes")
        @classmethod
        def validate_size_bytes(cls, v: int | None) -> int | None:
            if v is not None and v < 0:
                raise ValueError(f"Size in bytes cannot be negative: {v}")
            return v

    root: dict[LFN, CatalogEntry]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

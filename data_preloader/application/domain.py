"""
This module defines the core domain models for the application.

These classes represent the pure, technology-agnostic entities and data
structures that the application's business logic operates on.
"""

import dataclasses
from pathlib import Path

from abc import ABC, abstractmethod
from typing import List


# --- Domain Models ---

@dataclasses.dataclass(frozen=True)
class DumpMeta:
    """A transient data object for dump metadata from a data source."""

    url: str
    checksum: str
    size_bytes: int


@dataclasses.dataclass(frozen=True)
class ArchivedDump:
    """
    A domain model representing a downloaded archive file on disk,
    defined by its location and checksum.
    """

    path: Path
    checksum: str


@dataclasses.dataclass(frozen=True)
class ProcessedDump:
    """Domain model for a final, processed Parquet file on disk."""

    path: Path


# --- Ports (Interfaces) ---

class DataSource(ABC):
    """A port for any source of data dumps."""

    @abstractmethod
    async def get_dumps_info(
        self, modules: List[str], mode: str
    ) -> List[DumpMeta]:
        """Fetches metadata for available data dumps."""
        pass


class Downloader(ABC):
    """A port for any file downloader."""

    @abstractmethod
    async def download(
        self, meta: DumpMeta, destination: Path
    ) -> ArchivedDump:
        """Downloads a single dump to a destination path."""
        pass


class Hasher(ABC):
    """A port for hashing file contents."""

    @abstractmethod
    async def verify(self, archive: ArchivedDump):
        """
        Verifies the integrity of an archive.
        Raises VerificationError on mismatch.
        """
        pass


class Processor(ABC):
    """A port for processing a downloaded file."""

    @abstractmethod
    async def to_parquet(
        self, archive: ArchivedDump, destination: Path
    ) -> ProcessedDump:
        """Processes an archive and converts it to a Parquet file."""
        pass

"""
Infrastructure adapters for hashing and file processing tasks.
"""

import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Generator

import pandas
import pyarrow
import pyarrow.parquet as parquet
import zstandard

from ..application.domain import ArchivedDump, ProcessedDump, Hasher, Processor
from ..application.exceptions import ProcessingError, VerificationError


class Sha256Hasher(Hasher):
    """An adapter that implements the Hasher port using SHA256."""

    def __init__(self, force_check: bool = False, chunk_size: int = 65536):
        """Initializes the hasher."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.force_check = force_check
        self.chunk_size = chunk_size

    async def _calculate_sha256(self, file_path: Path) -> str:
        """Perform the blocking I/O work of hashing a file."""

        self.logger.info(f"Computing checksum for {file_path.name}...")

        hasher = hashlib.sha256()

        def _read_and_hash():
            with open(file_path, "rb") as f:
                while chunk := f.read(self.chunk_size):
                    hasher.update(chunk)
            return hasher.hexdigest()

        return await asyncio.to_thread(_read_and_hash)

    async def verify(self, archive: ArchivedDump):
        """
        Guarantee the archive is verified, checking hash only if necessary.

        This public method fulfills the Hasher port contract. It handles the
        idempotency check by looking for a '.verified' marker file and
        respects the 'force_check' flag to allow for re-verification.

        Args:
            archive: The ArchivedDump object representing the file to verify.

        Raises:
            VerificationError: If verification fails.
        """

        marker_path = archive.path.with_suffix(
            archive.path.suffix + ".verified"
        )

        if not self.force_check and marker_path.exists():
            self.logger.info(
                f"Checksum for {archive.path.name} already verified. Skipping."
            )
            return

        calculated_hash = await self._calculate_sha256(archive.path)

        if calculated_hash != archive.checksum:
            raise VerificationError(
                f"Checksum mismatch for {archive.path.name}. "
                f"Expected {archive.checksum}, got {calculated_hash}"
            )

        marker_path.touch()
        self.logger.info(
            f"Checksum for {archive.path.name} verified successfully."
        )


class TsvToParquetProcessor(Processor):
    """
    An adapter that implements the FileProcessor port to convert
    Zstandard-compressed TSV files to Parquet.
    """

    def __init__(self, required_columns, chunk_size=1_000_000):
        """Initializes the processor."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.required_columns = required_columns
        self.chunk_size = chunk_size

    def _decompress_and_read_chunks(
        self, source_path: Path
    ) -> Generator[pandas.DataFrame, None, None]:
        """
        Decompresses a Zstandard file and yields TSV data in chunks.
        """
        decompressor = zstandard.ZstdDecompressor()
        with open(source_path, "rb") as in_fh:
            with decompressor.stream_reader(in_fh) as reader:
                yield from pandas.read_csv(
                    reader,
                    sep="\t",
                    header=None,
                    names=self.required_columns,
                    usecols=self.required_columns,
                    chunksize=self.chunk_size,
                    encoding="utf-8",
                    dtype=str,
                )

    def _write_chunks_to_parquet(
        self, chunks: Generator[pandas.DataFrame, None, None], dest_path: Path
    ):
        """Consumes a generator of chunks and writes them to a Parquet file."""
        writer = None
        with open(dest_path, "wb") as out_fh:
            for chunk in chunks:
                table = pyarrow.Table.from_pandas(chunk)
                if writer is None:
                    writer = parquet.ParquetWriter(out_fh, table.schema)
                writer.write_table(table)
            if writer:
                writer.close()

    def _blocking_process_to_parquet(
        self, source_path: Path, dest_path: Path
    ):
        """
        Orchestrates the transformation from a compressed TSV to Parquet.
        """
        try:
            self.logger.info(f"Processing {dest_path.name} into Parquet...")
            chunks_generator = self._decompress_and_read_chunks(source_path)
            self._write_chunks_to_parquet(chunks_generator, dest_path)
            self.logger.info(f"Finished processing {dest_path.name}")
        except (pandas.errors.ParserError, KeyError, zstandard.ZstdError) as e:
            raise ProcessingError(
                f"Failed to process {source_path.name}: {e}"
            ) from e

    async def to_parquet(
        self, archive: ArchivedDump, destination: Path
    ) -> ProcessedDump:
        """
        Guarantee the Parquet file exists, processing only if necessary.

        This public method fulfills the Processor port contract. It handles
        the idempotency check and delegates the heavy, blocking I/O work to a
        separate thread to avoid blocking the async event loop.

        Args:
            archive: The ArchivedDump object to process.
            destination: The final path for the Parquet file.

        Returns:
            A ProcessedDump object representing the new file on disk.

        Raises:
            ProcessingError: If decompression, parsing, or writing fails.
        """

        if destination.exists():
            self.logger.info(
                f"Parquet file {destination.name} already exists. Skipping."
            )
        else:
            await asyncio.to_thread(
                self._blocking_process_to_parquet, archive.path, destination
            )

        return ProcessedDump(path=destination)

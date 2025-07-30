"""HTTP implementation of the Downloader port."""

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Generator, AsyncGenerator

import httpx
from tqdm import tqdm

from ..application.domain import ArchivedDump, DumpMeta, Downloader
from ..application.exceptions import DownloadError

from .base_client import BaseClient
from .decorators import retry_on_network_error


class HttpDownloader(BaseClient, Downloader):
    """A downloader that fetches files via HTTP atomically."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        token: str,
        timeout: int,
        chunk_size: int,
    ):
        """Initializes the downloader adapter."""
        super().__init__(client, token)
        self.timeout = timeout
        self.chunk_size = chunk_size

    @contextlib.contextmanager
    def _atomic_target(self, destination: Path) -> Generator[Path, None, None]:
        """Provides a temporary '.part' path and ensures cleanup."""
        part_path = destination.with_suffix(destination.suffix + ".part")
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            yield part_path
        finally:
            part_path.unlink(missing_ok=True)

    async def _stream_chunks(
        self, response: httpx.Response, target_file: Path,
    ):
        """Produce byte chunks from a response and write them to a file."""
        with open(target_file, "wb") as f:
            async for chunk in response.aiter_bytes(self.chunk_size):
                await asyncio.to_thread(f.write, chunk)
                yield len(chunk)

    async def _consume_stream_with_progress(
        self,
        stream: AsyncGenerator[int, None],
        total_size: int,
        desc: str,
    ):
        """Consume the byte stream to update a TQDM progress bar."""

        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc=desc
        ) as progress_bar:
            async for progress in stream:
                progress_bar.update(progress)

        if total_size != 0 and progress_bar.n != total_size:
            raise DownloadError(
                f"Size mismatch: {progress_bar.n} != {total_size}"
            )

    async def _stream_from_network(self, dump: DumpMeta, target_file: Path):
        """Manage the network request and the streaming process."""
        headers = {"Authorization": f"Bearer {self.token}"}
        async with self.client.stream(
            "GET", dump.url, timeout=self.timeout, headers=headers
        ) as response:
            response.raise_for_status()
            stream = self._stream_chunks(response, target_file)
            await self._consume_stream_with_progress(
                stream, dump.size_bytes, target_file.name
            )

    @retry_on_network_error
    async def _execute_atomic_download(self, dump: DumpMeta, destination: Path):
        """Orchestrate the entire atomic download operation."""
        self.logger.info(f"Downloading {destination.name}...")
        with self._atomic_target(destination) as part_path:
            await self._stream_from_network(dump, part_path)
            part_path.rename(destination)
        self.logger.info(f"Finished downloading {destination.name}")

    async def download(
        self, meta: DumpMeta, destination: Path
    ) -> ArchivedDump:
        """
        Guarantee that the archive file exists, downloading only if necessary.

        This is the public method that fulfills the Downloader port contract.
        It handles the idempotency check by verifying if the destination file
        already exists before delegating the actual work to private methods.

        Args:
            meta: The metadata of the dump to download.
            destination: The final desired path for the file.

        Returns:
            An ArchivedDump object representing the file on disk.

        Raises:
            DownloadError: If streaming download to file fails.
        """

        if destination.exists():
            self.logger.info(
                f"Archive {destination.name} already exists. Skipping download."
            )
        else:
            await self._execute_atomic_download(meta, destination)

        return ArchivedDump(path=destination, checksum=meta.checksum)

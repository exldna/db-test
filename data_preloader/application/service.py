"""
The core application service and pipeline, containing pure business logic.

This module defines the main orchestrator (PreloaderService) for the data
preloading process and the pipeline (DumpProcessingPipeline) that handles
the processing of a single data dump.
"""

import asyncio
import logging
from pathlib import Path
from typing import List

from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm.asyncio import tqdm_asyncio

from .domain import *

logger = logging.getLogger(__name__)


class DumpProcessingPipeline:
    """Encapsulates the full processing pipeline for a single dump."""

    def __init__(
        self,
        downloader: Downloader,
        hasher: Hasher,
        processor: Processor,
        download_cache_dir: Path,
        raw_data_dir: Path,
    ):
        """Initializes the pipeline with necessary dependencies (ports)."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.downloader = downloader
        self.hasher = hasher
        self.processor = processor
        self.download_cache_dir = download_cache_dir
        self.raw_data_dir = raw_data_dir

    async def run(self, dump: DumpMeta):
        """Executes the sequential steps for processing one dump.

        Args:
            dump: The metadata of the dump to process.
        """

        archive_name = dump.url.split("/")[-1]
        archive_path = self.download_cache_dir / archive_name
        parquet_path = self.raw_data_dir / archive_name
        parquet_path = parquet_path.with_suffix(".parquet")

        self.logger.info(f"Starting pipeline for {archive_name}...")

        # Step 1: Download (DumpMeta -> ArchivedDump)
        archive = await self.downloader.download(dump, archive_path)

        # Step 2: Verify (ArchivedDump -> void)
        await self.hasher.verify(archive)

        # Step 3: Process (ArchivedDump -> ProcessedDump)
        processed_dump = await self.processor.to_parquet(archive, parquet_path)

        self.logger.info(
            f"Successfully processed {processed_dump.path.name}"
        )


class PreloaderService:
    """Orchestrates the data preloading process by running pipelines."""

    def __init__(
        self,
        data_source: DataSource,
        downloader: Downloader,
        hasher: Hasher,
        processor: Processor,
        concurrent_downloads: int,
        download_cache_dir: Path,
        raw_data_dir: str,
    ):
        """Initializes the service and the reusable processing pipeline."""
        self.data_source = data_source
        self.concurrent_downloads = concurrent_downloads
        self.pipeline = DumpProcessingPipeline(
            downloader,
            hasher,
            processor,
            Path(download_cache_dir),
            Path(raw_data_dir),
        )

    async def _run_pipeline_with_semaphore(
        self, dump: DumpMeta, semaphore: asyncio.Semaphore
    ):
        """Wrapper to acquire a semaphore before running a pipeline."""
        async with semaphore:
            await self.pipeline.run(dump)

    async def run(self, modules: List[str], mode: str):
        """Executes the preloading process for all requested dumps."""

        logger.info(f"Starting preloader. Mode: {mode}, Modules: {modules}")

        dumps = await self.data_source.get_dumps_info(modules, mode)
        if not dumps:
            logger.info("No dumps found to process.")
            return

        semaphore = asyncio.Semaphore(self.concurrent_downloads)
        tasks = [
            asyncio.create_task(
                self._run_pipeline_with_semaphore(dump, semaphore)
            )
            for dump in dumps
        ]

        logger.info(
            f"Starting {len(tasks)} processing pipelines with a concurrency "
            f"limit of {self.concurrent_downloads}..."
        )

        with logging_redirect_tqdm():
            await tqdm_asyncio.gather(
                *tasks, desc="Overall Progress", unit="pipeline"
            )

        # await asyncio.gather(*tasks)

        logger.info("All processing tasks completed.")

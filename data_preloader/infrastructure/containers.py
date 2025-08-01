"""
Dependency Injection container for the data_preloader component.

This container uses the `dependency-injector` library to wire together all
the components of the application, such as services and infrastructure adapters,
based on the application's configuration.
"""

from dependency_injector import containers, providers
from dynaconf import Dynaconf
import httpx

from ..application.domain import *
from ..application.service import PreloaderService

from .api_client import HttpDataSource
from .downloader import HttpDownloader
from .processing import Sha256Hasher, TsvToParquetProcessor


class Container(containers.DeclarativeContainer):
    """DI container for wiring the application components."""

    cli_args = providers.Configuration()

    config = providers.Singleton(
        Dynaconf,
        settings_files=['config/settings.toml', 'config/.secrets.toml'],
        merge_enabled=True,
        load_dotenv=False,
        environments=False,
    )

    http_client = providers.Singleton(httpx.AsyncClient)

    data_source: providers.Factory[DataSource] = providers.Factory(
        HttpDataSource,
        client=http_client,
        token=config().preloader.api_token,
        base_url=config().preloader.api_base_url,
        timeout=config().preloader.timeout,
    )

    downloader: providers.Factory[Downloader] = providers.Factory(
        HttpDownloader,
        client=http_client,
        token=config().preloader.dump_token,
        timeout=config().preloader.timeout,
        chunk_size=config().preloader.downloader.chunk_size,
    )

    hasher: providers.Factory[Hasher] = providers.Factory(
        Sha256Hasher,
        force_check=cli_args.force_check,
        chunk_size=config().preloader.hasher.chunk_size,
    )

    processor: providers.Factory[Processor] = providers.Factory(
        TsvToParquetProcessor,
        chunk_size=config().preloader.processor.chunk_size,
        required_columns=config().preloader.processor.required_columns,
    )

    preloader_service = providers.Factory(
        PreloaderService,
        data_source=data_source,
        downloader=downloader,
        hasher=hasher,
        processor=processor,
        concurrent_downloads=config().preloader.concurrent_downloads,
        download_cache_dir=config().paths.download_cache_dir,
        raw_data_dir=config().paths.raw_data_dir,
    )

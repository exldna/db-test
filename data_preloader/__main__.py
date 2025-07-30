"""
Entry point for the data_preloader component.
"""

import argparse
import asyncio
import logging
import sys

from .application.exceptions import PreloaderError
from .infrastructure.containers import Container

logger = logging.getLogger(__name__)


def setup_logging(level: str):
    """Applies basic logging configuration."""
    logging.basicConfig(level=level)


async def run_application(args: argparse.Namespace):
    """Wires and runs the application using the DI container."""

    container = Container()
    container.cli_args.from_dict(vars(args))
    setup_logging(level=container.config().logging.level)
    preloader_service = container.preloader_service()

    try:
        await preloader_service.run(modules=args.modules, mode=args.mode)
    except PreloaderError as e:
        logger.error(f"An application error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Preloader Component")

    parser.add_argument(
        "--modules",
        required=True,
        nargs="+",
        help="A list of modules (blockchains) to download, e.g., bitcoin-main",
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=["latest", "all"],
        help="Download mode: 'latest' or 'all' available dumps.",
    )

    parser.add_argument(
        "--force-check",
        action="store_true",
        help="Force re-verification of existing files."
    )

    cli_args = parser.parse_args()

    asyncio.run(run_application(cli_args))

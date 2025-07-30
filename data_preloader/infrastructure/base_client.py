"""Base class for async HTTP clients."""

import logging
import httpx

from ..application.exceptions import ConfigurationError


class BaseClient:
    """A base client that handles an async client and token configuration."""

    def __init__(self, client: httpx.AsyncClient, token: str):
        """
        Initializes the base client.

        Args:
            client: An instance of httpx.AsyncClient.
            token: An authentication token.

        Raises:
            ConfigurationError: If the token is missing or appears to be
                                a placeholder.
        """

        if not token or "YOUR_" in token.upper():
            raise ConfigurationError(
                f"Authentication token for {self.__class__.__name__} is missing "
                f"or is a placeholder. Please check your config files."
            )

        self.client = client
        self.token = token
        self.logger = logging.getLogger(self.__class__.__name__)

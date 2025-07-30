"""HTTP implementation of the DataSource port."""

from typing import Any, Dict, List

import httpx

from ..application.domain import DumpMeta, DataSource
from ..application.exceptions import APIError

from .api_models import ApiResponse, DumpDetails
from .base_client import BaseClient
from .decorators import retry_on_network_error

_DUMPS_ENDPOINT = "/dumps"


class HttpDataSource(BaseClient, DataSource):
    """A data source that fetches dump information via an HTTP API."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        token: str,
        base_url: str,
        timeout: int
    ):
        """Initializes the data source adapter."""
        super().__init__(client, token)
        self.endpoint = base_url + _DUMPS_ENDPOINT
        self.timeout = timeout

    def _map_to_domain(self, dto: DumpDetails) -> DumpMeta:
        """Maps a single API DTO to a domain model."""
        return DumpMeta(
            url=dto.link,
            checksum=dto.checksum,
            size_bytes=dto.compressed_size,
        )

    async def _execute_fetch(self, params: Dict) -> Any:
        """Executes the raw HTTP GET request."""
        headers = {"Authorization": f"Bearer {self.token}"}
        response = await self.client.get(
            self.endpoint,
            params=params,
            headers=headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def _validate_and_extract(self, json_data: Any) -> List[DumpDetails]:
        """Validates raw response data and extracts a list of DTOs."""

        validated_response = ApiResponse.model_validate(json_data)

        if validated_response.context.code != 200:
            notice = validated_response.context.notice or "Unknown API error"
            raise APIError(
                f"API code {validated_response.context.code}: {notice}"
            )

        dtos = [
            details
            for date_dumps in validated_response.data.dumps.values()
            for details in date_dumps.values()
            if details.link
        ]

        return dtos

    @retry_on_network_error
    async def get_dumps_info(
        self, modules: List[str], mode: str
    ) -> List[DumpMeta]:
        """
        Orchestrates fetching, validating, and mapping dump information.

        This method serves as the public contract fulfillment for the
        DataSource port.

        Args:
            modules: A list of modules (blockchains) to fetch.
            mode: The download mode, either 'latest' or 'all'.

        Returns:
            A list of domain models representing the available dumps.

        Raises:
            APIError: If fetching metadata fails.
        """

        params = {"from": ",".join(modules), "mode": mode}
        self.logger.info(f"Fetching dump info for {params}...")

        raw_data = await self._execute_fetch(params)
        dump_dtos = self._validate_and_extract(raw_data)
        dumps = [self._map_to_domain(dto) for dto in dump_dtos]

        self.logger.info(
            f"Successfully processed info for {len(dumps)} dumps."
        )

        return dumps

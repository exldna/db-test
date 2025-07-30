"""
Pydantic models for validating the structure of responses from the 3xpl API.

These models serve as a strict contract for the expected JSON data, ensuring
that any deviation from this structure is caught at the infrastructure layer
before being passed to the application core.
"""

from typing import Dict, List, Optional

from pydantic import BaseModel


class DumpDetails(BaseModel):
    """
    Represents the metadata for a single data dump.

    Fields are marked as Optional because the API may return null for these
    values in historical entries, especially when using 'mode=all'. The
    application logic is responsible for filtering out entries where essential
    fields like 'link' are null.
    """

    link: Optional[str] = None
    checksum: Optional[str] = None
    compressed_size: Optional[int] = None
    uncompressed_size: Optional[int] = None
    checksum_algo: Optional[str] = None


class DumpsResponse(BaseModel):
    """Represents the nested 'dumps' object in the API response data."""

    dumps: Dict[str, Dict[str, DumpDetails]]


class ApiContext(BaseModel):
    """Represents the 'context' object containing metadata about the API call."""

    code: int
    notice: Optional[str] = None


class ApiResponse(BaseModel):
    """Represents the top-level structure of a successful API response."""

    data: DumpsResponse
    context: ApiContext

"""
Core business exceptions for the preloader application.

This module defines a hierarchy of custom exceptions to allow for granular
error handling and clear separation of failure domains.
"""


class PreloaderError(Exception):
    """Base exception for all component-specific errors."""
    pass


# --- Configuration Errors ---

class ConfigurationError(PreloaderError):
    """Raised for errors related to application configuration."""
    pass


# --- Infrastructure Errors ---

class InfrastructureError(PreloaderError):
    """Base class for errors related to external systems (network, API, etc.)."""
    pass


class APIError(InfrastructureError):
    """Raised for errors when communicating with the 3xpl API."""
    pass


class DownloadError(InfrastructureError):
    """Raised when a file download fails."""
    pass


# --- Domain/Business Logic Errors ---

class DomainError(PreloaderError):
    """Base class for errors related to business logic failures."""
    pass


class VerificationError(DomainError):
    """Raised when a verification step fails (e.g., checksum mismatch)."""
    pass


class ProcessingError(DomainError):
    """Raised when a file cannot be processed (e.g., decompression, parsing)."""
    pass

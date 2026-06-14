from __future__ import annotations

from typing import Any


class ErrorCode:
    INTERNAL_ERROR = "InternalError"
    METHOD_NOT_FOUND = "MethodNotFound"
    TIMEOUT = "Timeout"
    VALIDATION_ERROR = "ValidationError"


class RpcError(Exception):
    """
    Structured RPC error returned by the server.

    Raised by :meth:`RpcApp.call` when the server response contains an
    error payload.

    Attributes:
        error_code: Machine-readable error code (e.g. ``"MethodNotFound"``).
        error_message: Human-readable error description.
        error_details: Optional structured error metadata.
    """

    def __init__(
        self,
        error_code: str = ErrorCode.INTERNAL_ERROR,
        error_message: str = "",
        error_details: dict[str, Any] | None = None,
    ) -> None:
        """
        Args:
            error_code: Machine-readable error code.
            error_message: Human-readable error description.
            error_details: Optional dict with additional error context.
        """
        self.error_code = error_code
        self.error_message = error_message
        self.error_details = error_details
        super().__init__(f"[{error_code}] {error_message}")

from __future__ import annotations

from typing import Any


class ErrorCode:
    """
    Machine-readable error codes used in RPC error payloads and structured
    exceptions.
    """

    INTERNAL_ERROR = "InternalError"
    METHOD_NOT_FOUND = "MethodNotFound"
    TIMEOUT = "Timeout"
    VALIDATION_ERROR = "ValidationError"


class LinkrError(Exception):
    """
    Base class for all exceptions raised by linkr.

    Includes RPC errors, request/serialization errors, and other
    framework-level failures.
    """


class RPCError(LinkrError):
    """
    Error resulting from an RPC call.

    Raised by :meth:`App.call` when the server response contains an error
    payload or when the call times out waiting for a response.

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


class RequestError(LinkrError):
    """
    Local or transport-level error when preparing/sending a request.

    Raised for failures that are not tied to an RPC response, such as
    resolving an unknown serializer, serializing a request, or detecting a
    serializer for an incoming message on the server.

    Available for all delivery modes (RPC, task, event) when the error
    happens before the response is received.

    Attributes:
        error_message: Human-readable error description.
        error_details: Optional structured error metadata.
    """

    def __init__(
        self,
        error_message: str = "",
        error_details: dict[str, Any] | None = None,
    ) -> None:
        """
        Args:
            error_message: Human-readable error description.
            error_details: Optional dict with additional error context.
        """
        self.error_message = error_message
        self.error_details = error_details
        super().__init__(error_message)

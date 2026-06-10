from __future__ import annotations

from typing import Any


class RpcError(Exception):
    """
    Structured RPC error returned by the server.

    Attributes:
        error_code: Machine-readable error code (e.g. MethodNotFound).
        error_message: Human-readable error description.
        error_details: Optional structured error metadata.
    """

    def __init__(
        self,
        error_code: str = "InternalError",
        error_message: str = "",
        error_details: dict[str, Any] | None = None,
    ) -> None:
        self.error_code = error_code
        self.error_message = error_message
        self.error_details = error_details
        super().__init__(f"[{error_code}] {error_message}")

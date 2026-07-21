import importlib.metadata

from linkr.app import App, HandlerInfo, Invocation
from linkr.di import Depends, DiContainer, get_current_request
from linkr.exceptions import ErrorCode, RpcError
from linkr.middleware import AppMiddleware, BaseMiddleware, WireMiddleware
from linkr.models import ErrorInfo, Request, Response
from linkr.serializer import JsonRpcSerializer, JsonSerializer, Serializer
from linkr.transports import Transport
from linkr.transports.local import LocalTransport

__all__ = [
    "Depends",
    "DiContainer",
    "get_current_request",
    "Invocation",
    "HandlerInfo",
    "App",
    "ErrorCode",
    "ErrorInfo",
    "RpcError",
    "AppMiddleware",
    "BaseMiddleware",
    "WireMiddleware",
    "Request",
    "Response",
    "JsonRpcSerializer",
    "JsonSerializer",
    "Serializer",
    "Transport",
    "LocalTransport",
]


__version__ = importlib.metadata.version("linkr")

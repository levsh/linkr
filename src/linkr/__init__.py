import importlib.metadata

from linkr.app import HandlerInfo, RpcApp, RpcCall
from linkr.di import Depends, DiContainer
from linkr.exceptions import ErrorCode, RpcError
from linkr.middleware.base import AppMiddleware, BaseMiddleware, WireMiddleware
from linkr.middleware.gzip import GzipMiddleware
from linkr.models import ErrorInfo, RpcRequest, RpcResponse
from linkr.serializer import JsonRpcSerializer, JsonSerializer, Serializer
from linkr.transports import Transport
from linkr.transports.mock import MockTransport
from linkr.transports.rmq import RmqTransport, ThreadSafeRmqTransport

__all__ = [
    "Depends",
    "DiContainer",
    "RpcCall",
    "HandlerInfo",
    "RpcApp",
    "ErrorCode",
    "ErrorInfo",
    "RpcError",
    "AppMiddleware",
    "BaseMiddleware",
    "WireMiddleware",
    "GzipMiddleware",
    "RpcRequest",
    "RpcResponse",
    "JsonRpcSerializer",
    "JsonSerializer",
    "Serializer",
    "Transport",
    "MockTransport",
    "RmqTransport",
    "ThreadSafeRmqTransport",
]


__version__ = importlib.metadata.version("linkr")

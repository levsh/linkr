import importlib.metadata

from linkr.app import HandlerInfo, RpcApp, RpcCall
from linkr.di import Depends, DiContainer
from linkr.exceptions import RpcError
from linkr.middleware.base import AppMiddleware, BaseMiddleware, WireMiddleware
from linkr.middleware.gzip import GzipMiddleware
from linkr.models import RpcContext, RpcData, RpcHeaders, RpcRequest, RpcResponse
from linkr.serializer import JsonSerializer, Serializer
from linkr.transports import Transport
from linkr.transports.mock import MockTransport
from linkr.transports.rmq import RmqTransport, ThreadSafeRmqTransport

__all__ = [
    "Depends",
    "DiContainer",
    "RpcCall",
    "HandlerInfo",
    "RpcApp",
    "RpcError",
    "AppMiddleware",
    "BaseMiddleware",
    "WireMiddleware",
    "GzipMiddleware",
    "RpcRequest",
    "RpcResponse",
    "RpcContext",
    "RpcData",
    "RpcHeaders",
    "JsonSerializer",
    "Serializer",
    "Transport",
    "MockTransport",
    "RmqTransport",
    "ThreadSafeRmqTransport",
]


__version__ = importlib.metadata.version("linkr")

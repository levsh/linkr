import importlib.metadata

from linkr.app import App, RPCRouter, TaskRouter, get_current_request
from linkr.backends import Backend, Mode
from linkr.backends.local import LocalBackend
from linkr.di import Depends
from linkr.exceptions import ErrorCode, LinkrError, RequestError, RPCError
from linkr.hooks import (
    AfterHandlerEvent,
    AppHooks,
    BeforeHandlerEvent,
    ConsumeStartEvent,
    ConsumeStopEvent,
    HandlerErrorEvent,
    HookPoint,
    HookRegistry,
    MessageDroppedEvent,
    RequestPublishedEvent,
    RequestReceivedEvent,
    ResponseReceivedEvent,
)
from linkr.middleware import AppMiddleware, BaseMiddleware, WireMiddleware
from linkr.models import ErrorInfo, HandlerInfo, Message, QueueOptions, Request, RequestType, Response, ResponseType
from linkr.serializer import JsonRpcSerializer, JsonSerializer, Serializer

__all__ = [
    "Depends",
    "get_current_request",
    "HandlerInfo",
    "App",
    "RPCRouter",
    "TaskRouter",
    "ErrorCode",
    "ErrorInfo",
    "RPCError",
    "LinkrError",
    "RequestError",
    "AfterHandlerEvent",
    "AppHooks",
    "BeforeHandlerEvent",
    "ConsumeStartEvent",
    "ConsumeStopEvent",
    "HandlerErrorEvent",
    "HookPoint",
    "HookRegistry",
    "MessageDroppedEvent",
    "RequestPublishedEvent",
    "RequestReceivedEvent",
    "ResponseReceivedEvent",
    "AppMiddleware",
    "BaseMiddleware",
    "WireMiddleware",
    "Mode",
    "QueueOptions",
    "Message",
    "Request",
    "Response",
    "RequestType",
    "ResponseType",
    "JsonRpcSerializer",
    "JsonSerializer",
    "Serializer",
    "Backend",
    "LocalBackend",
]

try:
    __version__ = importlib.metadata.version("linkr")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.5.0"

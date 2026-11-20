import importlib.metadata

from linkr.app import App
from linkr.backends import Backend, Mode
from linkr.backends.local import LocalBackend
from linkr.client import Client
from linkr.di import Depends
from linkr.exceptions import (
    ErrorCode,
    LinkrError,
    RegistrationAlreadyExistsError,
    RegistrationNotFoundError,
    RequestError,
    RPCError,
)
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
from linkr.models import (
    ErrorInfo,
    HandlerInfo,
    HealthStatus,
    Message,
    QueueOptions,
    Request,
    RequestType,
    Response,
    ResponseType,
)
from linkr.serializer import JsonRpcSerializer, JsonSerializer, Serializer
from linkr.server import RPC, Bus, Worker, get_current_request

__all__ = [
    "App",
    "Backend",
    "Bus",
    "Client",
    "Depends",
    "ErrorCode",
    "ErrorInfo",
    "HandlerInfo",
    "HealthStatus",
    "LinkrError",
    "LocalBackend",
    "Mode",
    "RPC",
    "RegistrationAlreadyExistsError",
    "RegistrationNotFoundError",
    "Request",
    "RequestError",
    "RequestType",
    "Response",
    "ResponseType",
    "RPCError",
    "Serializer",
    "JsonSerializer",
    "JsonRpcSerializer",
    "WireMiddleware",
    "Worker",
    "get_current_request",
    "AfterHandlerEvent",
    "AppHooks",
    "AppMiddleware",
    "BaseMiddleware",
    "BeforeHandlerEvent",
    "ConsumeStartEvent",
    "ConsumeStopEvent",
    "HandlerErrorEvent",
    "HookPoint",
    "HookRegistry",
    "Message",
    "MessageDroppedEvent",
    "QueueOptions",
    "RequestPublishedEvent",
    "RequestReceivedEvent",
    "ResponseReceivedEvent",
]

try:
    __version__ = importlib.metadata.version("linkr")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.8.1"

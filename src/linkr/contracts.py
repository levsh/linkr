from __future__ import annotations

import html
import inspect
import json
from collections.abc import Callable, ItemsView
from datetime import UTC, datetime
from types import NoneType
from typing import Annotated, Any, Literal, get_type_hints

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from pydantic.json_schema import GenerateJsonSchema

from .models import HandlerInfo

CONTRACT_VERSION = "linkr/1.0-alpha"


class ContractParam(BaseModel):
    """Parameter specification in a handler contract."""

    model_config = ConfigDict(extra="forbid")

    name: str
    kind: str | None = None
    position: int | None = None
    required: bool
    json_schema: dict[str, Any] = Field(default_factory=dict)
    default: Any = None
    has_default: bool = Field(default=False, exclude=True)


class HandlerInputContract(BaseModel):
    """Input contract for RPC/task handlers or event payload shape."""

    model_config = ConfigDict(extra="forbid")

    params: list[ContractParam] = Field(default_factory=list)
    args_schema: dict[str, Any] = Field(default_factory=dict)
    kwds_schema: dict[str, Any] = Field(default_factory=dict)


class HandlerOutputContract(BaseModel):
    """Output contract for RPC handlers."""

    model_config = ConfigDict(extra="forbid")

    json_schema: dict[str, Any] = Field(default_factory=dict)


class _BaseMethodContract(BaseModel):
    """Common fields for RPC/task contract entries."""

    model_config = ConfigDict(extra="forbid")

    name: str
    queue: str
    description: str | None = None
    input: HandlerInputContract = Field(default_factory=HandlerInputContract)
    validate_types: bool = False


class RpcMethodContract(_BaseMethodContract):
    """Contract entry for an RPC method."""

    kind: Literal["rpc"] = "rpc"
    call: str
    output: HandlerOutputContract = Field(default_factory=HandlerOutputContract)


class TaskMethodContract(_BaseMethodContract):
    """Contract entry for a task handler."""

    kind: Literal["task"] = "task"
    send: str
    output: None = None


MethodContract = Annotated[RpcMethodContract | TaskMethodContract, Field(discriminator="kind")]


class EventPayloadContract(BaseModel):
    """Payload contract for an event subscriber (keyword arguments)."""

    model_config = ConfigDict(extra="forbid")

    params: list[ContractParam] = Field(default_factory=list)
    kwds_schema: dict[str, Any] = Field(default_factory=dict)


class EventSubscriberContract(BaseModel):
    """Contract entry for a single event subscriber."""

    model_config = ConfigDict(extra="forbid")

    queue_template: str
    handler: str
    description: str | None = None
    payload: EventPayloadContract = Field(default_factory=EventPayloadContract)
    validate_types: bool = False


class EventContract(BaseModel):
    """Contract entry for an event topic pattern and its subscribers."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["event"] = "event"
    topic: str
    subscribers: list[EventSubscriberContract] = Field(default_factory=list)


class ComponentsContract(BaseModel):
    """Reusable schema components collected during contract building."""

    model_config = ConfigDict(extra="forbid")

    schemas: dict[str, Any] = Field(default_factory=dict)


class AppContract(BaseModel):
    """Machine-readable contract/specification for a linkr application."""

    model_config = ConfigDict(extra="forbid")

    contract: str = CONTRACT_VERSION
    app: str | None = None
    methods: dict[str, RpcMethodContract] = Field(default_factory=dict)
    tasks: dict[str, TaskMethodContract] = Field(default_factory=dict)
    events: dict[str, EventContract] = Field(default_factory=dict)
    components: ComponentsContract = Field(default_factory=ComponentsContract)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible contract dictionary (stable wire shape)."""

        def dump_param(param: ContractParam) -> dict[str, Any]:
            data: dict[str, Any] = {
                "name": param.name,
                "required": param.required,
                "schema": param.json_schema,
            }
            if param.kind is not None:
                data["kind"] = param.kind
            if param.position is not None:
                data["position"] = param.position
            if param.has_default:
                data["default"] = param.default
            return data

        def dump_method(method: RpcMethodContract | TaskMethodContract) -> dict[str, Any]:
            data: dict[str, Any] = {
                "kind": method.kind,
                "name": method.name,
                "queue": method.queue,
                "description": method.description,
                "input": {
                    "params": [dump_param(p) for p in method.input.params],
                    "args_schema": method.input.args_schema,
                    "kwds_schema": method.input.kwds_schema,
                },
                "validate_types": method.validate_types,
            }
            if isinstance(method, RpcMethodContract):
                data["call"] = method.call
                data["output"] = {"schema": method.output.json_schema}
            else:
                data["send"] = method.send
                data["output"] = None
            return data

        def dump_event(event: EventContract) -> dict[str, Any]:
            return {
                "kind": event.kind,
                "topic": event.topic,
                "subscribers": [
                    {
                        "queue_template": sub.queue_template,
                        "handler": sub.handler,
                        "description": sub.description,
                        "payload": {
                            "params": [dump_param(p) for p in sub.payload.params],
                            "kwds_schema": sub.payload.kwds_schema,
                        },
                        "validate_types": sub.validate_types,
                    }
                    for sub in event.subscribers
                ],
            }

        return {
            "contract": self.contract,
            "app": self.app,
            "methods": {key: dump_method(value) for key, value in self.methods.items()},
            "tasks": {key: dump_method(value) for key, value in self.tasks.items()},
            "events": {key: dump_event(value) for key, value in self.events.items()},
            "components": {"schemas": self.components.schemas},
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        """Serialize the contract to JSON using the stable wire shape."""
        return json.dumps(self.to_dict(), indent=indent, default=ContractBuilder.json_default)

    def to_html(self, *, title: str | None = None) -> str:
        """Render the contract as a self-contained HTML page."""
        return render_contract_html(self, title=title)


class ContractBuilder:
    """Builder that assembles an :class:`AppContract` from an application instance."""

    class _SchemaGenerator(GenerateJsonSchema):
        schema_dialect = "https://json-schema.org/draft/2020-12/schema"

    def __init__(
        self,
        app: Any,
        *,
        include: set[str] | None = None,
        include_internal: bool = False,
    ) -> None:
        self._app = app
        self._include = include or {"methods", "tasks", "events"}
        self._include_internal = include_internal
        self._components: dict[str, Any] = {}

    def build(self) -> AppContract:
        """Build and return the application contract model."""
        contract = AppContract(app=getattr(self._app, "name", None))
        if "methods" in self._include:
            contract.methods = self._build_methods()
        if "tasks" in self._include:
            contract.tasks = self._build_tasks()
        if "events" in self._include:
            contract.events = self._build_events()
        contract.components = ComponentsContract(schemas=self._components)
        return contract

    def _build_methods(self) -> dict[str, RpcMethodContract]:
        methods: dict[str, RpcMethodContract] = {}
        for (queue, leaf), info in sorted(
            self._iter_handlers("_rpc_handlers"), key=lambda item: (item[0][0], item[0][1])
        ):
            fq_name = f"{queue}.{leaf}"
            if not self._include_internal and self._is_internal_name(fq_name):
                continue
            methods[fq_name] = self._build_rpc_handler(info, fq_name=fq_name, queue=queue)
        return methods

    def _build_tasks(self) -> dict[str, TaskMethodContract]:
        tasks: dict[str, TaskMethodContract] = {}
        for (queue, leaf), info in sorted(
            self._iter_handlers("_task_handlers"), key=lambda item: (item[0][0], item[0][1])
        ):
            fq_name = f"{queue}.{leaf}"
            if not self._include_internal and self._is_internal_name(fq_name):
                continue
            tasks[fq_name] = self._build_task_handler(info, fq_name=fq_name, queue=queue)
        return tasks

    def _build_events(self) -> dict[str, EventContract]:
        events: dict[str, EventContract] = {}
        subscribers_by_pattern = getattr(self._app, "_subscribers", {})
        app_name = getattr(self._app, "name", "")

        grouped: dict[str, list[HandlerInfo]] = {}
        for _compiled, infos in subscribers_by_pattern.items():
            for info in infos:
                grouped.setdefault(info.name, []).append(info)

        for topic in sorted(grouped):
            subscribers: list[EventSubscriberContract] = []
            for info in grouped[topic]:
                fn = inspect.unwrap(info.fn)
                params, _args_schema, kwds_schema = self._build_input(info, fn, is_event=True)
                subscribers.append(
                    EventSubscriberContract(
                        queue_template=f"{app_name}.<instance_id>",
                        handler=getattr(info.fn, "__name__", info.name),
                        description=inspect.getdoc(fn),
                        payload=EventPayloadContract(params=params, kwds_schema=kwds_schema),
                        validate_types=bool(getattr(info, "validate_args", False)),
                    )
                )
            events[topic] = EventContract(topic=topic, subscribers=subscribers)
        return events

    def _build_handler(
        self,
        info: HandlerInfo,
        *,
        fq_name: str,
        queue: str,
        kind: Literal["rpc", "task"],
    ) -> RpcMethodContract | TaskMethodContract:
        if kind == "rpc":
            return self._build_rpc_handler(info, fq_name=fq_name, queue=queue)
        return self._build_task_handler(info, fq_name=fq_name, queue=queue)

    def _build_rpc_handler(
        self,
        info: HandlerInfo,
        *,
        fq_name: str,
        queue: str,
    ) -> RpcMethodContract:
        fn = inspect.unwrap(info.fn)
        params, args_schema, kwds_schema = self._build_input(info, fn, is_event=False)
        return RpcMethodContract(
            name=info.name.rsplit(".", 1)[-1],
            queue=queue,
            call=fq_name,
            description=inspect.getdoc(fn),
            input=HandlerInputContract(
                params=params,
                args_schema=args_schema,
                kwds_schema=kwds_schema,
            ),
            output=HandlerOutputContract(json_schema=self._build_output_schema(fn)),
            validate_types=bool(getattr(info, "validate_args", False)),
        )

    def _build_task_handler(
        self,
        info: HandlerInfo,
        *,
        fq_name: str,
        queue: str,
    ) -> TaskMethodContract:
        fn = inspect.unwrap(info.fn)
        params, args_schema, kwds_schema = self._build_input(info, fn, is_event=False)
        return TaskMethodContract(
            name=info.name.rsplit(".", 1)[-1],
            queue=queue,
            send=fq_name,
            description=inspect.getdoc(fn),
            input=HandlerInputContract(
                params=params,
                args_schema=args_schema,
                kwds_schema=kwds_schema,
            ),
            output=None,
            validate_types=bool(getattr(info, "validate_args", False)),
        )

    def _build_input(
        self,
        info: HandlerInfo,
        fn: Callable[..., Any],
        *,
        is_event: bool,
    ) -> tuple[list[ContractParam], dict[str, Any], dict[str, Any]]:
        try:
            sig = inspect.signature(fn)
        except (TypeError, ValueError):
            sig = None
        try:
            hints = get_type_hints(fn, include_extras=True)
        except Exception:
            hints = {}

        dep_types = getattr(info, "dep_types", {}) or {}
        params: list[ContractParam] = []
        properties: dict[str, Any] = {}
        required: list[str] = []
        prefix_items: list[dict[str, Any]] = []
        args_items_schema: dict[str, Any] | None = None
        additional_positional = False
        additional_keywords = False

        if sig is None:
            return [], {"type": "array"}, {"type": "object"}

        positional_kinds = (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )

        position = 0
        for pname, param in sig.parameters.items():
            if pname in dep_types:
                continue

            annotation = hints.get(pname, inspect.Parameter.empty)
            schema = self._schema_for_annotation(annotation)
            has_default = param.default is not inspect.Parameter.empty
            default_value = self._safe_json_value(param.default) if has_default else None
            default_ok = has_default and default_value is not self._UNSERIALIZABLE

            if param.kind in positional_kinds:
                params.append(
                    ContractParam(
                        name=pname,
                        kind=self._param_kind(param.kind),
                        position=position,
                        required=not has_default,
                        json_schema=schema,
                        default=default_value if default_ok else None,
                        has_default=default_ok,
                    )
                )
                properties[pname] = schema
                if not has_default:
                    required.append(pname)
                prefix_items.append(schema)
                position += 1
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                params.append(
                    ContractParam(
                        name=pname,
                        kind=self._param_kind(param.kind),
                        required=not has_default,
                        json_schema=schema,
                        default=default_value if default_ok else None,
                        has_default=default_ok,
                    )
                )
                properties[pname] = schema
                if not has_default:
                    required.append(pname)
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                additional_positional = True
                args_items_schema = schema
                params.append(
                    ContractParam(
                        name=pname,
                        kind=self._param_kind(param.kind),
                        required=False,
                        json_schema=schema,
                    )
                )
            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                additional_keywords = True
                params.append(
                    ContractParam(
                        name=pname,
                        kind=self._param_kind(param.kind),
                        required=False,
                        json_schema=schema,
                    )
                )

        kwds_schema: dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            kwds_schema["required"] = required
        if additional_keywords:
            extra_schema = None
            for p in params:
                if p.kind == "var_keyword":
                    extra_schema = p.json_schema
                    break
            kwds_schema["additionalProperties"] = extra_schema if extra_schema is not None else True
        else:
            kwds_schema["additionalProperties"] = False

        args_schema: dict[str, Any] = {"type": "array"}
        if prefix_items:
            args_schema["prefixItems"] = prefix_items
        if additional_positional:
            args_schema["items"] = args_items_schema if args_items_schema is not None else True
        elif prefix_items:
            args_schema["items"] = False
            args_schema["maxItems"] = len(prefix_items)
        else:
            args_schema["maxItems"] = 0

        if is_event:
            return params, {"type": "array", "maxItems": 0}, kwds_schema
        return params, args_schema, kwds_schema

    def _build_output_schema(self, fn: Callable[..., Any]) -> dict[str, Any]:
        try:
            hints = get_type_hints(fn, include_extras=True)
        except Exception:
            hints = {}
        annotation = hints.get("return", inspect.Parameter.empty)
        if annotation is inspect.Parameter.empty:
            return {}
        if annotation is None or annotation is NoneType:
            return {"type": "null"}
        return self._schema_for_annotation(annotation)

    def _schema_for_annotation(self, annotation: Any) -> dict[str, Any]:
        if annotation is inspect.Parameter.empty or annotation is Any:
            return {}
        try:
            adapter = TypeAdapter(annotation)
            schema = adapter.json_schema(schema_generator=self._SchemaGenerator, mode="validation")
        except Exception:
            return {"x-python-type": self._safe_repr(annotation)}

        if not isinstance(schema, dict):
            return {"x-python-type": self._safe_repr(annotation)}

        schema = json.loads(json.dumps(schema, default=self.json_default))
        defs = schema.pop("$defs", None)
        if isinstance(defs, dict):
            for key, value in defs.items():
                if key not in self._components:
                    self._components[key] = value
                    schema = self._rewrite_ref(schema, f"#/$defs/{key}", f"#/components/schemas/{key}")
                    continue
                if self._components[key] != value:
                    new_key = self._unique_def_key(key)
                    self._components[new_key] = value
                    schema = self._rewrite_ref(schema, f"#/$defs/{key}", f"#/components/schemas/{new_key}")
                    continue
                schema = self._rewrite_ref(schema, f"#/$defs/{key}", f"#/components/schemas/{key}")
        schema = self._rewrite_ref_prefix(schema, "#/$defs/", "#/components/schemas/")
        return schema

    def _unique_def_key(self, key: str) -> str:
        i = 2
        while f"{key}_{i}" in self._components:
            i += 1
        return f"{key}_{i}"

    def _iter_handlers(self, attr: str) -> ItemsView[tuple[str, str], HandlerInfo]:
        handlers = getattr(self._app, attr, {})
        return handlers.items()

    def _is_internal_name(self, _name: str) -> bool:
        # Reserved for future internal handlers; respects include_internal flag.
        return False

    @classmethod
    def _rewrite_ref(cls, schema: Any, old: str, new: str) -> Any:
        if isinstance(schema, dict):
            return {
                k: (new if k == "$ref" and v == old else cls._rewrite_ref(v, old, new))
                for k, v in schema.items()
            }
        if isinstance(schema, list):
            return [cls._rewrite_ref(item, old, new) for item in schema]
        return schema

    @classmethod
    def _rewrite_ref_prefix(cls, schema: Any, old_prefix: str, new_prefix: str) -> Any:
        if isinstance(schema, dict):
            out = {}
            for k, v in schema.items():
                if k == "$ref" and isinstance(v, str) and v.startswith(old_prefix):
                    out[k] = new_prefix + v[len(old_prefix) :]
                else:
                    out[k] = cls._rewrite_ref_prefix(v, old_prefix, new_prefix)
            return out
        if isinstance(schema, list):
            return [cls._rewrite_ref_prefix(item, old_prefix, new_prefix) for item in schema]
        return schema

    @staticmethod
    def _param_kind(kind: inspect._ParameterKind) -> str:
        mapping = {
            inspect.Parameter.POSITIONAL_ONLY: "positional_only",
            inspect.Parameter.POSITIONAL_OR_KEYWORD: "positional_or_keyword",
            inspect.Parameter.VAR_POSITIONAL: "var_positional",
            inspect.Parameter.KEYWORD_ONLY: "keyword_only",
            inspect.Parameter.VAR_KEYWORD: "var_keyword",
        }
        return mapping.get(kind, str(kind))

    _UNSERIALIZABLE = object()

    @classmethod
    def _safe_json_value(cls, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, (list, tuple)):
            items = []
            for item in value:
                v = cls._safe_json_value(item)
                if v is cls._UNSERIALIZABLE:
                    return cls._UNSERIALIZABLE
                items.append(v)
            return items
        if isinstance(value, dict):
            out = {}
            for k, v in value.items():
                sv = cls._safe_json_value(v)
                if sv is cls._UNSERIALIZABLE or not isinstance(k, str):
                    return cls._UNSERIALIZABLE
                out[k] = sv
            return out
        try:
            json.dumps(value)
            return value
        except Exception:
            return cls._UNSERIALIZABLE

    @staticmethod
    def _safe_repr(obj: Any) -> str:
        try:
            return repr(obj)
        except Exception:
            return "<unrepresentable>"

    @staticmethod
    def json_default(value: Any) -> Any:
        """JSON fallback serializer for contract values."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            return value.isoformat()
        if isinstance(value, (set, frozenset)):
            return sorted(value, key=str)
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        try:
            return str(value)
        except Exception:
            return "<unserializable>"


class HtmlContractRenderer:
    """Render an :class:`~linkr.contracts.AppContract` as a self-contained HTML page."""

    def __init__(self, contract: AppContract | dict[str, Any], *, title: str | None = None) -> None:
        self.contract_data = contract.to_dict() if isinstance(contract, AppContract) else dict(contract)
        self.title = title or f"Contract — {self.contract_data.get('app') or 'app'}"

    def render(self) -> str:
        """Return a complete HTML document for the contract."""
        data = self.contract_data
        methods = data.get("methods") or {}
        tasks = data.get("tasks") or {}
        events = data.get("events") or {}
        components = ((data.get("components") or {}).get("schemas")) or {}
        return "\n".join(
            [
                "<!doctype html>",
                '<html lang="en">',
                "<head>",
                self._head(),
                "</head>",
                "<body>",
                self._header(methods, tasks, events),
                '<div class="layout">',
                self._sidebar(methods, tasks, events),
                '<main class="content">',
                self._section_methods("Methods (RPC)", methods, kind="rpc"),
                self._section_methods("Tasks", tasks, kind="task"),
                self._section_events(events),
                self._section_components(components),
                "</main>",
                "</div>",
                self._script(),
                "</body>",
                "</html>",
            ]
        )

    def _e(self, value: Any) -> str:
        return html.escape("" if value is None else str(value), quote=True)

    def _json(self, value: Any) -> str:
        return html.escape(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=False))

    def _type_label(self, schema: Any) -> str:
        if not isinstance(schema, dict) or not schema:
            if isinstance(schema, dict) and "x-python-type" in schema:
                return str(schema["x-python-type"])
            return "any"
        if "x-python-type" in schema and len(schema) == 1:
            return str(schema["x-python-type"])
        if "$ref" in schema:
            ref = str(schema["$ref"])
            return ref.rsplit("/", 1)[-1]
        if "const" in schema:
            return f"const({schema['const']!r})"
        if "enum" in schema:
            values = schema["enum"]
            vals = ", ".join(repr(v) for v in values[:4])
            suffix = ", …" if len(values) > 4 else ""
            return f"enum({vals}{suffix})"
        for key in ("anyOf", "oneOf"):
            if key in schema and isinstance(schema[key], list):
                return " | ".join(self._type_label(item) for item in schema[key])
        if "allOf" in schema and isinstance(schema["allOf"], list):
            return " & ".join(self._type_label(item) for item in schema["allOf"])
        type_value = schema.get("type")
        if isinstance(type_value, list):
            return " | ".join(str(item) for item in type_value)
        if type_value == "array":
            if "prefixItems" in schema and isinstance(schema["prefixItems"], list):
                inner = ", ".join(self._type_label(item) for item in schema["prefixItems"])
                return f"tuple[{inner}]"
            items = schema.get("items") or {}
            return f"array[{self._type_label(items)}]"
        if type_value:
            fmt = schema.get("format")
            return f"{type_value}({fmt})" if fmt else str(type_value)
        title = schema.get("title")
        return str(title) if title else "any"

    def _details(self, label: str, data: Any, *, open_: bool = False) -> str:
        if data is None:
            return ""
        attr = " open" if open_ else ""
        return (
            f'<details class="schema"{attr}><summary>{self._e(label)}</summary>'
            f"<pre>{self._json(data)}</pre></details>"
        )

    def _badges(self, *items: tuple[str, str]) -> str:
        return "".join(
            f'<span class="badge badge-{self._e(css_class)}">{self._e(text)}</span>'
            for text, css_class in items
        )

    def _params_table(self, params: list[dict[str, Any]]) -> str:
        if not params:
            return '<p class="muted">No parameters.</p>'
        rows = []
        for param in params:
            kind = str(param.get("kind") or "")
            pos = param.get("position")
            kind_pos = kind if pos is None else f"{kind} (pos {pos})"
            default = "—" if "default" not in param else json.dumps(param.get("default"), ensure_ascii=False)
            required = "yes" if param.get("required") else "no"
            schema = param.get("schema") or {}
            rows.append(
                "<tr>"
                f"<td><code>{self._e(param.get('name'))}</code></td>"
                f"<td>{self._e(kind_pos)}</td>"
                f'<td class="req-{required}">{required}</td>'
                f"<td><code>{self._e(default)}</code></td>"
                f"<td><code>{self._e(self._type_label(schema))}</code></td>"
                "</tr>"
            )
        return (
            '<table class="params"><thead><tr>'
            "<th>Name</th><th>Kind</th><th>Required</th><th>Default</th><th>Type</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
        )

    def _header(self, methods: dict[str, Any], tasks: dict[str, Any], events: dict[str, Any]) -> str:
        data = self.contract_data
        return (
            '<header class="topbar"><div>'
            f'<h1>{self._e(data.get("app") or "app")}</h1>'
            '<div class="meta">'
            f'{self._badges((str(data.get("contract", "")), "ver"))}'
            f"<span>{len(methods)} methods · {len(tasks)} tasks · {len(events)} event topics</span>"
            "</div></div></header>"
        )

    def _sidebar(self, methods: dict[str, Any], tasks: dict[str, Any], events: dict[str, Any]) -> str:
        def links(items: dict[str, Any], prefix: str) -> str:
            return "".join(
                f'<li><a href="#{self._e(prefix + name)}">{self._e(name)}</a></li>'
                for name in sorted(items)
            )

        return (
            '<nav class="sidebar"><div class="sidebar-inner">'
            '<input id="q" type="search" placeholder="Filter…" oninput="filterOps(this.value)" />'
            f'<h3>Methods</h3><ul class="nav-list">{links(methods, "m-")}</ul>'
            f'<h3>Tasks</h3><ul class="nav-list">{links(tasks, "t-")}</ul>'
            f'<h3>Events</h3><ul class="nav-list">{links(events, "e-")}</ul>'
            '<h3>Sections</h3><ul class="nav-list"><li><a href="#components">Components</a></li></ul>'
            "</div></nav>"
        )

    def _section_methods(self, title: str, items: dict[str, Any], *, kind: str) -> str:
        if not items:
            return f'<section><h2>{self._e(title)}</h2><p class="muted">None.</p></section>'
        id_prefix = "m-" if kind == "rpc" else "t-"
        cards = []
        for fq_name, op in sorted(items.items()):
            input_contract = op.get("input") or {}
            output_contract = op.get("output")
            description = op.get("description")
            address = op.get("call") or op.get("send") or fq_name
            filter_text = " ".join(
                str(part)
                for part in [fq_name, address, op.get("queue"), op.get("name"), description or ""]
            )
            badges = [(kind.upper(), kind), (f"queue: {op.get('queue')}", "queue")]
            if op.get("validate_types"):
                badges.append(("validate_types", "val"))
            schema_details = self._details(
                "Input schema (args / kwds)",
                {
                    "args_schema": input_contract.get("args_schema"),
                    "kwds_schema": input_contract.get("kwds_schema"),
                },
            )
            if kind == "rpc":
                result = self._details("Result schema", output_contract)
            else:
                result = '<p class="muted">No result (fire-and-forget).</p>'
            cards.append(
                f'<article class="card op" id="{self._e(id_prefix + fq_name)}" '
                f'data-filter-text="{self._e(filter_text)}">'
                '<div class="card-head">'
                f"<h3><code>{self._e(address)}</code></h3>"
                f'<div class="badges">{self._badges(*badges)}</div>'
                "</div>"
                + (f"<p>{self._e(description)}</p>" if description else "")
                + "<h4>Parameters</h4>"
                + self._params_table(input_contract.get("params") or [])
                + schema_details
                + result
                + "</article>"
            )
        return f'<section><h2>{self._e(title)}</h2>{"".join(cards)}</section>'

    def _section_events(self, events: dict[str, Any]) -> str:
        if not events:
            return '<section><h2>Events</h2><p class="muted">None.</p></section>'
        cards = []
        for topic, event in sorted(events.items()):
            subscribers_html = []
            for subscriber in event.get("subscribers") or []:
                payload = subscriber.get("payload") or {}
                badges = [(f"queue: {subscriber.get('queue_template')}", "queue")]
                if subscriber.get("validate_types"):
                    badges.append(("validate_types", "val"))
                description = subscriber.get("description")
                subscribers_html.append(
                    '<div class="subscriber">'
                    '<div class="sub-head">'
                    f"<code>{self._e(subscriber.get('handler'))}</code>"
                    f'<div class="badges">{self._badges(*badges)}</div>'
                    "</div>"
                    + (f"<p>{self._e(description)}</p>" if description else "")
                    + self._params_table(payload.get("params") or [])
                    + self._details("Payload schema (kwds)", {"kwds_schema": payload.get("kwds_schema")})
                    + "</div>"
                )
            filter_text = " ".join(
                [str(topic)]
                + [str(sub.get("handler", "")) for sub in event.get("subscribers") or []]
            )
            cards.append(
                f'<article class="card op" id="{self._e("e-" + topic)}" '
                f'data-filter-text="{self._e(filter_text)}">'
                '<div class="card-head">'
                f"<h3><code>{self._e(topic)}</code></h3>"
                f'<div class="badges">{self._badges(("EVENT", "event"))}</div>'
                "</div>"
                + ("".join(subscribers_html) or '<p class="muted">No subscribers.</p>')
                + "</article>"
            )
        return f'<section><h2>Events</h2>{"".join(cards)}</section>'

    def _section_components(self, schemas: dict[str, Any]) -> str:
        if not schemas:
            return (
                '<section id="components"><h2>Components (schemas)</h2>'
                '<p class="muted">None.</p></section>'
            )
        blocks = [
            self._details(name, schema)
            for name, schema in sorted(schemas.items())
        ]
        return (
            '<section id="components"><h2>Components (schemas)</h2>'
            + "".join(blocks)
            + "</section>"
        )

    def _head(self) -> str:
        return f"""<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{self._e(self.title)}</title>
<style>
:root{{
  --bg:#f6f7f9;--card:#fff;--text:#1b1f24;--muted:#667085;--border:#e4e7ec;
  --accent:#2563eb;--mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
}}
*{{box-sizing:border-box}}
body{{
  margin:0;font:14px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
  color:var(--text);background:var(--bg);
}}
.topbar{{
  position:sticky;top:0;z-index:2;background:var(--card);
  border-bottom:1px solid var(--border);padding:14px 20px;
}}
.topbar h1{{margin:0;font-size:20px}}
.meta{{color:var(--muted);display:flex;gap:10px;align-items:center;margin-top:4px;flex-wrap:wrap}}
.layout{{display:grid;grid-template-columns:280px 1fr;min-height:calc(100vh - 70px)}}
.sidebar{{border-right:1px solid var(--border);background:#fbfbfc}}
.sidebar-inner{{position:sticky;top:70px;padding:16px;max-height:calc(100vh - 70px);overflow:auto}}
.sidebar h3{{margin:14px 0 6px;font-size:12px;text-transform:uppercase;color:var(--muted)}}
.nav-list{{list-style:none;padding:0;margin:0}}
.nav-list li{{margin:4px 0}}
.nav-list a{{color:var(--text);text-decoration:none}}
.nav-list a:hover{{color:var(--accent)}}
.sidebar input{{width:100%;padding:8px 10px;border:1px solid var(--border);border-radius:8px}}
.content{{padding:20px;max-width:1100px}}
section{{margin-bottom:28px}}
h2{{margin:0 0 10px}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px 16px;margin:12px 0}}
.card-head{{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap}}
.card h3{{margin:0;font-size:16px}}
h4{{margin:12px 0 6px}}
.badges{{display:flex;gap:6px;flex-wrap:wrap}}
.badge{{font-size:12px;padding:2px 8px;border-radius:999px;background:#eef2ff;color:#3730a3;border:1px solid #e0e7ff}}
.badge-rpc{{background:#ecfeff;color:#155e75;border-color:#cffafe}}
.badge-task{{background:#fff7ed;color:#9a3412;border-color:#ffedd5}}
.badge-event{{background:#f0fdf4;color:#166534;border-color:#dcfce7}}
.badge-queue,.badge-val,.badge-ver{{background:#f2f4f7;color:#344054;border-color:#e4e7ec}}
table.params{{width:100%;border-collapse:collapse;margin:8px 0}}
th,td{{border-bottom:1px solid var(--border);padding:8px 6px;text-align:left;vertical-align:top}}
th{{font-size:12px;color:var(--muted);text-transform:uppercase}}
code,pre{{font-family:var(--mono)}}
pre{{background:#0b1020;color:#e6edf3;padding:12px;border-radius:8px;overflow:auto;margin:8px 0}}
details.schema{{margin:8px 0}}
details.schema>summary{{cursor:pointer;color:var(--accent);font-weight:600}}
.muted{{color:var(--muted)}}
.subscriber{{border-top:1px dashed var(--border);margin-top:10px;padding-top:10px}}
.sub-head{{display:flex;gap:8px;align-items:center;justify-content:space-between;flex-wrap:wrap}}
@media (max-width:900px){{
  .layout{{grid-template-columns:1fr}}
  .sidebar{{border-right:0;border-bottom:1px solid var(--border)}}
  .sidebar-inner{{position:static;max-height:none}}
}}
@media print{{
  .sidebar{{display:none}}
  .layout{{grid-template-columns:1fr}}
  .topbar{{position:static}}
}}
</style>"""

    def _script(self) -> str:
        return """<script>
function filterOps(query) {
  const q = (query || "").toLowerCase();
  document.querySelectorAll("[data-filter-text]").forEach((el) => {
    const text = (el.dataset.filterText || "").toLowerCase();
    el.style.display = !q || text.includes(q) ? "" : "none";
  });
}
</script>"""


def render_contract_html(contract: AppContract | dict[str, Any], *, title: str | None = None) -> str:
    """Render a contract model or contract dictionary as a self-contained HTML page."""
    return HtmlContractRenderer(contract, title=title).render()


def describe_app(
    app: Any,
    *,
    include: set[str] | None = None,
    include_internal: bool = False,
) -> AppContract:
    """Build and return an :class:`AppContract` model for *app*."""
    return ContractBuilder(app, include=include, include_internal=include_internal).build()


def describe_app_json(app: Any, *, indent: int | None = 2, **kwargs: Any) -> str:
    """Serialize :func:`describe_app` to a JSON string."""
    return describe_app(app, **kwargs).to_json(indent=indent)

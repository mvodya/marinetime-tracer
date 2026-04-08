#!/usr/bin/env python3

# Run:
# python scripts/export_public_api.py --package mtlib --format compact --output api.txt

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import pkgutil
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from types import ModuleType
from typing import Any, get_type_hints


# Utils
def safe_repr(obj: Any) -> str:
    try:
        return repr(obj)
    except Exception:
        return f"<unreprable:{type(obj).__name__}>"


def annotation_to_str(annotation: Any) -> str:
    if annotation is inspect.Signature.empty:
        return ""
    try:
        if isinstance(annotation, type):
            return annotation.__name__
        return str(annotation).replace("typing.", "")
    except Exception:
        return safe_repr(annotation)


def clean_doc(doc: str | None) -> str:
    if not doc:
        return ""
    first = inspect.cleandoc(doc).splitlines()
    return first[0].strip() if first else ""


def is_public_name(name: str) -> bool:
    return not name.startswith("_")


def is_defined_in_module(obj: Any, module_name: str) -> bool:
    return getattr(obj, "__module__", None) == module_name


def signature_to_dict(obj: Any) -> dict[str, Any]:
    try:
        sig = inspect.signature(obj)
    except Exception:
        return {
            "signature": "<unavailable>",
            "params": [],
            "return": "",
        }

    params = []
    for p in sig.parameters.values():
        param_info = {
            "name": p.name,
            "kind": str(p.kind).replace("Parameter.", ""),
            "annotation": annotation_to_str(p.annotation),
            "default": "" if p.default is inspect.Signature.empty else safe_repr(p.default),
        }
        params.append(param_info)

    return {
        "signature": str(sig),
        "params": params,
        "return": annotation_to_str(sig.return_annotation),
    }


# Data models
@dataclass
class ApiFunction:
    name: str
    qualname: str
    signature: str
    params: list[dict[str, Any]]
    returns: str
    doc: str
    kind: str = "function"


@dataclass
class ApiMethod:
    name: str
    qualname: str
    signature: str
    params: list[dict[str, Any]]
    returns: str
    doc: str
    kind: str = "method"


@dataclass
class ApiClass:
    name: str
    qualname: str
    doc: str
    methods: list[ApiMethod]
    kind: str = "class"


@dataclass
class ApiModule:
    module: str
    functions: list[ApiFunction]
    classes: list[ApiClass]


# Introspection
def collect_module_functions(module: ModuleType) -> list[ApiFunction]:
    items: list[ApiFunction] = []

    for name, obj in inspect.getmembers(module, inspect.isfunction):
        if not is_public_name(name):
            continue
        if not is_defined_in_module(obj, module.__name__):
            continue

        sig = signature_to_dict(obj)
        items.append(
            ApiFunction(
                name=name,
                qualname=f"{module.__name__}.{name}",
                signature=sig["signature"],
                params=sig["params"],
                returns=sig["return"],
                doc=clean_doc(inspect.getdoc(obj)),
            )
        )

    return items


def collect_class_methods(cls: type) -> list[ApiMethod]:
    items: list[ApiMethod] = []

    for name, obj in inspect.getmembers(cls):
        if not is_public_name(name):
            continue

        if inspect.isfunction(obj) or inspect.ismethod(obj):
            if getattr(obj, "__qualname__", "").split(".")[0] != cls.__name__:
                continue

            sig = signature_to_dict(obj)
            items.append(
                ApiMethod(
                    name=name,
                    qualname=f"{cls.__module__}.{cls.__name__}.{name}",
                    signature=sig["signature"],
                    params=sig["params"],
                    returns=sig["return"],
                    doc=clean_doc(inspect.getdoc(obj)),
                )
            )

    return items


def collect_module_classes(module: ModuleType) -> list[ApiClass]:
    items: list[ApiClass] = []

    for name, obj in inspect.getmembers(module, inspect.isclass):
        if not is_public_name(name):
            continue
        if not is_defined_in_module(obj, module.__name__):
            continue

        items.append(
            ApiClass(
                name=name,
                qualname=f"{module.__name__}.{name}",
                doc=clean_doc(inspect.getdoc(obj)),
                methods=collect_class_methods(obj),
            )
        )

    return items


def iter_package_modules(package_name: str):
    pkg = importlib.import_module(package_name)
    yield pkg

    if hasattr(pkg, "__path__"):
        for mod in pkgutil.walk_packages(pkg.__path__, prefix=pkg.__name__ + "."):
            yield importlib.import_module(mod.name)


def collect_api(package_name: str) -> list[ApiModule]:
    result: list[ApiModule] = []

    for module in iter_package_modules(package_name):
        functions = collect_module_functions(module)
        classes = collect_module_classes(module)

        if functions or classes:
            result.append(
                ApiModule(
                    module=module.__name__,
                    functions=functions,
                    classes=classes,
                )
            )

    return result


# Compact render
def render_compact(api: list[ApiModule]) -> str:
    lines: list[str] = []

    for mod in api:
        lines.append(f"[{mod.module}]")

        for fn in mod.functions:
            lines.append(f"f {fn.name}{fn.signature}")

        for cls in mod.classes:
            lines.append(f"c {cls.name}")
            for method in cls.methods:
                lines.append(f"  m {method.name}{method.signature}")

        lines.append("")

    return "\n".join(lines).strip()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export public API of a Python package"
    )
    parser.add_argument(
        "--package",
        default="mtlib",
        help="Package name to inspect (default: mtlib)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "compact"],
        default="json",
        help="Output format",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output file path. If omitted, print to stdout.",
    )

    args = parser.parse_args()

    api = collect_api(args.package)

    if args.format == "json":
        payload = [asdict(x) for x in api]
        text = json.dumps(payload, ensure_ascii=False, indent=2)
    else:
        text = render_compact(api)

    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
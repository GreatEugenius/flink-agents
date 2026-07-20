################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import builtins
import importlib.abc
import importlib.util
import os
import sys
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from importlib.machinery import ModuleSpec, PathFinder
from pathlib import Path
from types import ModuleType
from typing import Any, Iterator

_ARTIFACT_PATH_ATTRIBUTE = "__flink_agents_artifact_path__"
_FINDER_MARKER = "__flink_agents_module_namespace_finder__"
_FRAMEWORK_PACKAGE = "flink_agents"
_NAMESPACE_USERS: dict[str, int] = {}
_NAMESPACE_LOCK = threading.RLock()
_ORIGINAL_IMPORT = builtins.__import__
_CURRENT_NAMESPACE: ContextVar[str | None] = ContextVar(
    "flink_agents_plan_module_namespace", default=None
)


@contextmanager
def bind(namespace: str | None) -> Iterator[None]:
    """Bind the plan namespace used while runtime objects are constructed."""
    token = _CURRENT_NAMESPACE.set(namespace)
    try:
        yield
    finally:
        _CURRENT_NAMESPACE.reset(token)


def current() -> str | None:
    """Return the namespace bound to the current execution context."""
    return _CURRENT_NAMESPACE.get()


def register(namespace: str, artifact_path: str | Path) -> ModuleType:
    """Register an artifact as a synthetic package root."""
    if not namespace.isidentifier():
        msg = f"Invalid Python module namespace: {namespace!r}"
        raise ValueError(msg)

    with _NAMESPACE_LOCK:
        normalized_path = os.path.realpath(os.fspath(artifact_path))
        if not Path(normalized_path).exists():
            msg = f"Python artifact path does not exist: {normalized_path}"
            raise ValueError(msg)

        existing = sys.modules.get(namespace)
        if existing is not None:
            registered_path = existing.__dict__.get(_ARTIFACT_PATH_ATTRIBUTE)
            if registered_path != normalized_path:
                msg = (
                    f"Python module namespace {namespace!r} is already registered "
                    f"for {registered_path!r}, not {normalized_path!r}"
                )
                raise ValueError(msg)
            return existing

        package = ModuleType(namespace)
        package.__package__ = namespace
        package.__path__ = [normalized_path]
        setattr(package, _ARTIFACT_PATH_ATTRIBUTE, normalized_path)
        spec = ModuleSpec(namespace, loader=None, is_package=True)
        spec.submodule_search_locations = [normalized_path]
        package.__spec__ = spec
        sys.modules[namespace] = package
        return package


def acquire(namespace: str, artifact_path: str | Path) -> ModuleType:
    """Acquire one runtime lease on a shared namespace package."""
    with _NAMESPACE_LOCK:
        if _NAMESPACE_USERS.get(namespace, 0) == 0 and _is_loaded(namespace):
            drop(namespace)
        package = register(namespace, artifact_path)
        _NAMESPACE_USERS[namespace] = _NAMESPACE_USERS.get(namespace, 0) + 1
        return package


def release(namespace: str, drop_if_unused: bool = False) -> bool:  # noqa: FBT001
    """Release one runtime lease and optionally drop an unused namespace."""
    with _NAMESPACE_LOCK:
        users = _NAMESPACE_USERS.get(namespace, 0)
        if users <= 0:
            msg = f"Python module namespace {namespace!r} has no runtime lease"
            raise ValueError(msg)

        users -= 1
        if users > 0:
            _NAMESPACE_USERS[namespace] = users
            return False

        del _NAMESPACE_USERS[namespace]
        if drop_if_unused:
            drop(namespace)
            return True
        return False


def qualify(namespace: str | None, logical_module: str) -> str:
    """Map an artifact-owned logical module into a plan namespace."""
    if namespace is None:
        return logical_module
    if logical_module == namespace or logical_module.startswith(namespace + "."):
        return logical_module
    if logical_module == _FRAMEWORK_PACKAGE or logical_module.startswith(
        _FRAMEWORK_PACKAGE + "."
    ):
        return logical_module

    top_level_module = logical_module.split(".", 1)[0]
    try:
        artifact_spec = importlib.util.find_spec(f"{namespace}.{top_level_module}")
    except (ModuleNotFoundError, ValueError):
        artifact_spec = None
    if artifact_spec is None:
        return logical_module
    return f"{namespace}.{logical_module}"


def drop(namespace: str) -> None:
    """Force-remove exactly one synthetic namespace tree from ``sys.modules``."""
    with _NAMESPACE_LOCK:
        _NAMESPACE_USERS.pop(namespace, None)
        for loaded_name in list(sys.modules):
            if loaded_name == namespace or loaded_name.startswith(namespace + "."):
                del sys.modules[loaded_name]


def usage_count(namespace: str) -> int:
    """Return the number of runtimes currently leasing ``namespace``."""
    with _NAMESPACE_LOCK:
        return _NAMESPACE_USERS.get(namespace, 0)


def _is_loaded(namespace: str) -> bool:
    return any(
        name == namespace or name.startswith(namespace + ".") for name in sys.modules
    )


def _private_builtins(namespace: str) -> dict[str, Any]:
    module_builtins = dict(vars(builtins))

    def namespace_import(
        name: str,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> ModuleType:
        resolved_name = qualify(namespace, name) if level == 0 else name
        imported = _ORIGINAL_IMPORT(resolved_name, globals, locals, fromlist, level)
        if level == 0 and resolved_name != name and not fromlist:
            top_level_module = name.split(".", 1)[0]
            return sys.modules[f"{namespace}.{top_level_module}"]
        return imported

    module_builtins["__import__"] = namespace_import
    return module_builtins


class _NamespaceLoader(importlib.abc.Loader):
    def __init__(self, delegate: Any, namespace: str) -> None:
        self._delegate = delegate
        self._namespace = namespace

    def create_module(self, spec: ModuleSpec) -> ModuleType | None:
        create_module = getattr(self._delegate, "create_module", None)
        return create_module(spec) if create_module is not None else None

    def exec_module(self, module: ModuleType) -> None:
        module.__dict__["__builtins__"] = _private_builtins(self._namespace)
        exec_module = getattr(self._delegate, "exec_module", None)
        if exec_module is None:
            msg = f"Loader for {module.__name__!r} cannot execute modules"
            raise ImportError(msg)
        exec_module(module)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class _NamespaceFinder(importlib.abc.MetaPathFinder):
    def __init__(self) -> None:
        setattr(self, _FINDER_MARKER, True)

    def find_spec(
        self,
        fullname: str,
        path: list[str] | None = None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        namespace, separator, _ = fullname.partition(".")
        if not separator:
            return None
        package = sys.modules.get(namespace)
        if (
            package is None
            or package.__dict__.get(_ARTIFACT_PATH_ATTRIBUTE) is None
        ):
            return None

        spec = PathFinder.find_spec(fullname, path, target)
        if spec is not None and spec.loader is not None:
            spec.loader = _NamespaceLoader(spec.loader, namespace)
        return spec


def _install_finder() -> None:
    if any(getattr(finder, _FINDER_MARKER, False) for finder in sys.meta_path):
        return
    sys.meta_path.insert(0, _NamespaceFinder())


_install_finder()

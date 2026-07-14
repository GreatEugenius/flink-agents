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
"""Switches the shared module world between dynamic-plan Python artifacts.

All pemja interpreters of one process run in ``MULTI_THREAD`` mode and share a
single CPython interpreter, so ``sys.modules`` and ``sys.path`` are one global
layer for every plan. User code is therefore modeled as two layers:

* the *base layer* — code distributed at job submission, always on ``sys.path``;
* the *overlay* — the current dynamic-plan artifact, prepended to ``sys.path``.

:func:`activate_plan_artifact` is the single switch point. It must only run when
no user Python code of the retired plan can execute concurrently (the operator
guarantees this: it is called after the pre-barrier drain). Eviction rules:

1. every module loaded from a retired artifact root is evicted (the overlay is
   replaced wholesale — this covers all transitive imports of an action,
   because the criterion is the module's file location, not import reachability);
2. every top-level name *provided by* the new artifact is evicted regardless of
   where it was loaded from — this is what lets an artifact shadow base-layer
   modules that were already imported.

Base-layer modules not shadowed by the new artifact keep their identity: they
are neither evicted nor reloaded, so references held elsewhere stay valid.
"""

import importlib
import logging
import os
import sys
from pathlib import Path
from typing import Iterable, List, Set

logger = logging.getLogger(__name__)

# Names that must never be evicted: the framework itself holds live state
# (runner contexts, adapters) inside these modules, and pemja bridges into them.
_PROTECTED_TOP_LEVEL = {
    "flink_agents",
    "pemja",
    "function",
    "flink_runner_context",
    "python_java_utils",
}


def _canonical(path: str) -> str:
    return os.path.realpath(path)


def _is_under(origin: str, roots: List[str]) -> bool:
    origin = os.path.realpath(origin)
    return any(origin == root or origin.startswith(root + os.sep) for root in roots)


def _module_origins(module: object) -> List[str]:
    """Best-effort file locations of a module: spec origin, __file__, and, for
    (namespace) packages, every __path__ entry.
    """
    origins = []
    spec = getattr(module, "__spec__", None)
    origin = getattr(spec, "origin", None) or getattr(module, "__file__", None)
    if isinstance(origin, str):
        origins.append(origin)
    paths = getattr(module, "__path__", None)
    if paths is not None:
        origins.extend(p for p in list(paths) if isinstance(p, str))
    return origins


def provided_top_level_names(root: str) -> Set[str]:
    """Importable top-level names an artifact root provides: ``<name>.py`` files
    and directories containing at least one ``.py`` file (regular or namespace
    packages).
    """
    names = set()
    try:
        entries = list(Path(root).iterdir())
    except OSError:
        return names
    for entry in entries:
        if entry.is_file() and entry.name.endswith(".py"):
            names.add(entry.name[: -len(".py")])
        elif entry.is_dir() and _contains_python_file(str(entry)):
            names.add(entry.name)
    return {name for name in names if name.isidentifier()}


def _contains_python_file(path: str) -> bool:
    for _dirpath, _dirnames, filenames in os.walk(path):
        if any(f.endswith(".py") for f in filenames):
            return True
    return False


def activate_plan_artifact(
    old_artifact_roots: Iterable[str],
    new_artifact_roots: Iterable[str],
    path_only_roots: Iterable[str] = (),
    retired_function_scope: str | None = None,
) -> List[str]:
    """Retire the old artifact overlay and activate the new one.

    Parameters
    ----------
    old_artifact_roots : Iterable[str]
        Artifact roots of the plan being retired. Their modules are evicted
        (rule 1) and the paths removed from ``sys.path``.
    new_artifact_roots : Iterable[str]
        Artifact roots of the plan being activated. Top-level names they
        provide are evicted from ``sys.modules`` (rule 2) and the paths are
        prepended to ``sys.path``.
    path_only_roots : Iterable[str]
        Paths prepended in front of the artifact roots without any eviction
        (the ``dynamic-plan.python-runtime.path`` layer). Already-imported
        framework modules are intentionally not reloadable in-process.
    retired_function_scope : str | None
        Function-cache scope of the retired plan runtime; matching entries in
        the plan-function cache are dropped.

    Returns:
    -------
    List[str]
        Sorted names of the evicted modules, for logging on the caller side.
    """
    old_roots = [_canonical(r) for r in old_artifact_roots or () if r]
    new_roots = [_canonical(r) for r in new_artifact_roots or () if r]
    front_roots = [_canonical(r) for r in path_only_roots or () if r]
    retired_roots = [r for r in old_roots if r not in new_roots]

    evicted = set()

    # Rule 1: the retired overlay is replaced wholesale — evict by file origin.
    if retired_roots:
        for name, module in list(sys.modules.items()):
            origins = _module_origins(module)
            if origins and any(_is_under(o, retired_roots) for o in origins):
                evicted.add(name)

    # Rule 2: names shadowed by the new artifact lose their cached entry, no
    # matter which layer they were loaded from.
    shadowed = set()
    for root in new_roots:
        shadowed |= provided_top_level_names(root)
    blocked = shadowed & _PROTECTED_TOP_LEVEL
    if blocked:
        logger.warning(
            "Artifact provides protected top-level names %s; these are not "
            "reloadable in-process and keep their already-imported modules.",
            sorted(blocked),
        )
        shadowed -= _PROTECTED_TOP_LEVEL
    for name in list(sys.modules):
        if name.split(".", 1)[0] in shadowed:
            evicted.add(name)

    evicted = {
        name for name in evicted if name.split(".", 1)[0] not in _PROTECTED_TOP_LEVEL
    }
    for name in evicted:
        sys.modules.pop(name, None)

    # Swap sys.path: drop retired overlay paths, then put the new overlay (and
    # the path-only layer in front of it) at the head.
    remove = set(retired_roots) | set(new_roots) | set(front_roots)
    sys.path[:] = [p for p in sys.path if _canonical(p) not in remove]
    for root in reversed(front_roots + new_roots):
        sys.path.insert(0, root)
    importlib.invalidate_caches()

    if retired_function_scope:
        from flink_agents.plan import function as plan_function

        plan_function.evict_python_function_scope(retired_function_scope)

    return sorted(evicted)

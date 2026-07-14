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
import importlib
import sys
import textwrap
from pathlib import Path
from typing import Iterator

import pytest

from flink_agents.plan.function import (
    _PYTHON_FUNCTION_CACHE,
    call_python_function,
    evict_python_function_scope,
)
from flink_agents.runtime.plan_artifact import (
    activate_plan_artifact,
    provided_top_level_names,
)

_TEST_MODULES = (
    "util_mod",
    "actions_mod",
    "helper_mod",
    "shared_lib",
    "ns_pkg",
    "flink_agents_fake",
)


@pytest.fixture(autouse=True)
def _isolate_import_state() -> Iterator[None]:
    """Snapshot and restore sys.path and the sys.modules entries this test suite
    creates, so eviction tests cannot leak into other tests.
    """
    saved_path = list(sys.path)
    yield
    sys.path[:] = saved_path
    for name in list(sys.modules):
        if name.split(".", 1)[0] in _TEST_MODULES:
            del sys.modules[name]
    importlib.invalidate_caches()


def _write_module(root: Path, relpath: str, content: str) -> None:
    file = root / relpath
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(textwrap.dedent(content))


def _make_base_layer(tmp_path: Path) -> Path:
    base = tmp_path / "base"
    _write_module(base, "util_mod.py", "VERSION = 'base'\n")
    _write_module(base, "shared_lib.py", "VERSION = 'base'\n")
    return base


def _make_artifact(tmp_path: Path, name: str, version: str) -> Path:
    artifact = tmp_path / name
    _write_module(artifact, "util_mod.py", f"VERSION = '{version}'\n")
    _write_module(
        artifact,
        "actions_mod.py",
        f"""
        import util_mod
        import helper_mod

        VERSION = '{version}'

        def which_helper():
            return helper_mod.VERSION
        """,
    )
    _write_module(artifact, "helper_mod.py", f"VERSION = '{version}'\n")
    return artifact


def test_provided_top_level_names(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact"
    _write_module(artifact, "util_mod.py", "")
    _write_module(artifact, "pkg/__init__.py", "")
    _write_module(artifact, "ns_pkg/sub.py", "")
    (artifact / "empty_dir").mkdir()
    (artifact / "not-an-identifier.py").write_text("")
    (artifact / "data.txt").write_text("")

    assert provided_top_level_names(str(artifact)) == {"util_mod", "pkg", "ns_pkg"}


def test_artifact_shadows_already_imported_base_module(tmp_path: Path) -> None:
    base = _make_base_layer(tmp_path)
    sys.path.insert(0, str(base))
    assert importlib.import_module("util_mod").VERSION == "base"

    artifact = _make_artifact(tmp_path, "artifact_v2", "v2")
    evicted = activate_plan_artifact([], [str(artifact)])

    # Rule 2: the cached base-layer module was evicted because the artifact
    # provides the same top-level name; a fresh import resolves the overlay.
    assert "util_mod" in evicted
    assert importlib.import_module("util_mod").VERSION == "v2"


def test_transitive_imports_of_action_are_replaced(tmp_path: Path) -> None:
    base = _make_base_layer(tmp_path)
    sys.path.insert(0, str(base))

    artifact_v2 = _make_artifact(tmp_path, "artifact_v2", "v2")
    activate_plan_artifact([], [str(artifact_v2)])
    actions = importlib.import_module("actions_mod")
    assert actions.which_helper() == "v2"

    artifact_v3 = _make_artifact(tmp_path, "artifact_v3", "v3")
    evicted = activate_plan_artifact([str(artifact_v2)], [str(artifact_v3)])

    # Rule 1: helper_mod was only ever imported transitively by actions_mod,
    # yet it is evicted because its file lives under the retired artifact.
    assert {"actions_mod", "helper_mod", "util_mod"} <= set(evicted)
    actions = importlib.import_module("actions_mod")
    assert actions.VERSION == "v3"
    assert actions.which_helper() == "v3"


def test_unshadowed_base_module_keeps_identity(tmp_path: Path) -> None:
    base = _make_base_layer(tmp_path)
    sys.path.insert(0, str(base))
    shared_before = importlib.import_module("shared_lib")

    artifact = _make_artifact(tmp_path, "artifact_v2", "v2")
    evicted = activate_plan_artifact([], [str(artifact)])

    # shared_lib is not provided by the artifact: not evicted, not reloaded.
    assert "shared_lib" not in evicted
    assert sys.modules["shared_lib"] is shared_before


def test_sys_path_overlay_swap(tmp_path: Path) -> None:
    artifact_v2 = _make_artifact(tmp_path, "artifact_v2", "v2")
    artifact_v3 = _make_artifact(tmp_path, "artifact_v3", "v3")
    runtime_root = tmp_path / "runtime_layer"
    runtime_root.mkdir()

    activate_plan_artifact([], [str(artifact_v2)])
    assert sys.path[0] == str(artifact_v2)

    activate_plan_artifact([str(artifact_v2)], [str(artifact_v3)], [str(runtime_root)])
    assert sys.path[0] == str(runtime_root)
    assert sys.path[1] == str(artifact_v3)
    assert str(artifact_v2) not in sys.path


def test_namespace_package_is_evicted_by_origin(tmp_path: Path) -> None:
    artifact_v2 = tmp_path / "artifact_v2"
    _write_module(artifact_v2, "ns_pkg/sub.py", "VERSION = 'v2'\n")
    activate_plan_artifact([], [str(artifact_v2)])
    assert importlib.import_module("ns_pkg.sub").VERSION == "v2"

    artifact_v3 = tmp_path / "artifact_v3"
    _write_module(artifact_v3, "ns_pkg/sub.py", "VERSION = 'v3'\n")
    evicted = activate_plan_artifact([str(artifact_v2)], [str(artifact_v3)])

    assert {"ns_pkg", "ns_pkg.sub"} <= set(evicted)
    assert importlib.import_module("ns_pkg.sub").VERSION == "v3"


def test_protected_framework_modules_are_never_evicted(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact"
    _write_module(artifact, "flink_agents/__init__.py", "")
    _write_module(artifact, "util_mod.py", "VERSION = 'v2'\n")

    framework_before = sys.modules["flink_agents"]
    evicted = activate_plan_artifact([], [str(artifact)])

    assert all(not name.startswith("flink_agents") for name in evicted)
    assert sys.modules["flink_agents"] is framework_before


def test_evict_python_function_scope_drops_only_matching_scope(
    tmp_path: Path,
) -> None:
    artifact = _make_artifact(tmp_path, "artifact_v2", "v2")
    activate_plan_artifact([], [str(artifact)])

    _write_module(
        tmp_path / "artifact_v2",
        "func_mod.py",
        """
        def get_version():
            return 'v2'
        """,
    )
    assert call_python_function("scope-a", "func_mod", "get_version", ()) == "v2"
    assert call_python_function("func_mod", "get_version", ()) == "v2"
    assert ("scope-a", "func_mod", "get_version") in _PYTHON_FUNCTION_CACHE
    assert ("func_mod", "get_version") in _PYTHON_FUNCTION_CACHE

    removed = evict_python_function_scope("scope-a")

    assert removed == 1
    assert ("scope-a", "func_mod", "get_version") not in _PYTHON_FUNCTION_CACHE
    assert ("func_mod", "get_version") in _PYTHON_FUNCTION_CACHE
    del _PYTHON_FUNCTION_CACHE[("func_mod", "get_version")]
    sys.modules.pop("func_mod", None)


def test_scope_eviction_via_activate(tmp_path: Path) -> None:
    artifact = _make_artifact(tmp_path, "artifact_v2", "v2")
    activate_plan_artifact([], [str(artifact)])
    _write_module(
        tmp_path / "artifact_v2",
        "func_mod.py",
        """
        def get_version():
            return 'v2'
        """,
    )
    call_python_function("scope-b", "func_mod", "get_version", ())
    assert ("scope-b", "func_mod", "get_version") in _PYTHON_FUNCTION_CACHE

    artifact_v3 = _make_artifact(tmp_path, "artifact_v3", "v3")
    activate_plan_artifact(
        [str(artifact)], [str(artifact_v3)], retired_function_scope="scope-b"
    )

    assert ("scope-b", "func_mod", "get_version") not in _PYTHON_FUNCTION_CACHE
    sys.modules.pop("func_mod", None)

from __future__ import annotations

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
#################################################################################
import importlib
import json
import sys
import types
import zipfile
import zipimport
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Generator, Tuple

import pytest

import flink_agents.plan.function as function_module
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext
from flink_agents.plan.function import (
    JavaFunction,
    PythonFunction,
    _is_function_cacheable,
    _python_artifact_roots,
    call_python_function,
    clear_python_function_cache,
    get_python_function_cache_keys,
    get_python_function_cache_size,
    switch_python_artifact,
)

if TYPE_CHECKING:
    from flink_agents.api.function import Function


def check_class(input_event: InputEvent, output_event: OutputEvent) -> None:
    pass


def test_function_signature_same_class() -> None:
    func = PythonFunction.from_callable(check_class)
    func.check_signature(InputEvent, OutputEvent)


def test_function_signature_subclass() -> None:
    func = PythonFunction.from_callable(check_class)
    func.check_signature(Event, Event)


def test_function_signature_mismatch_class() -> None:
    func = PythonFunction.from_callable(check_class)
    with pytest.raises(TypeError):
        func.check_signature(OutputEvent, InputEvent)


def test_function_signature_mismatch_args_num() -> None:
    func = PythonFunction.from_callable(check_class)
    with pytest.raises(TypeError):
        func.check_signature(InputEvent)


def check_primitive(value: int) -> None:
    pass


def test_function_signature_same_primitive() -> None:
    func = PythonFunction.from_callable(check_primitive)
    func.check_signature(int)


def test_function_signature_mismatch_primitive() -> None:
    func = PythonFunction.from_callable(check_primitive)
    with pytest.raises(TypeError):
        func.check_signature(float)


def check_mix(a: int, b: InputEvent) -> None:
    pass


def test_function_signature_match_mix() -> None:
    func = PythonFunction.from_callable(check_mix)
    func.check_signature(int, Event)


def test_function_signature_mismatch_mix() -> None:
    func = PythonFunction.from_callable(check_mix)
    with pytest.raises(TypeError):
        func.check_signature(Event, int)


def check_generic_type(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    pass


def test_function_signature_generic_type_same() -> None:
    func = PythonFunction.from_callable(check_generic_type)
    func.check_signature(Tuple[Any, ...], Dict[str, Any])


def test_function_signature_generic_type_match() -> None:
    func = PythonFunction.from_callable(check_generic_type)
    func.check_signature(tuple, dict)


def test_function_signature_generic_type_mismatch() -> None:
    func = PythonFunction.from_callable(check_generic_type)
    with pytest.raises(TypeError):
        func.check_signature(Tuple[str, ...], Dict[str, Any])


# ── JavaFunction signature checks ───────────────────────────────────────


def _java_action_function() -> JavaFunction:
    return JavaFunction(
        qualname="com.example.Handlers",
        method_name="handle",
        parameter_types=[
            "org.apache.flink.agents.api.Event",
            "org.apache.flink.agents.api.context.RunnerContext",
        ],
    )


def test_java_function_signature_matching_arity_passes() -> None:
    _java_action_function().check_signature(Event, RunnerContext)


def test_java_function_signature_arity_too_few_raises() -> None:
    func = JavaFunction(
        qualname="com.example.Handlers",
        method_name="handle",
        parameter_types=["org.apache.flink.agents.api.Event"],
    )
    with pytest.raises(TypeError, match="declares 1 parameter type"):
        func.check_signature(Event, RunnerContext)


def test_java_function_signature_arity_too_many_raises() -> None:
    func = JavaFunction(
        qualname="com.example.Handlers",
        method_name="handle",
        parameter_types=[
            "org.apache.flink.agents.api.Event",
            "org.apache.flink.agents.api.context.RunnerContext",
            "java.lang.String",
        ],
    )
    with pytest.raises(TypeError, match="declares 3 parameter type"):
        func.check_signature(Event, RunnerContext)


current_dir = Path(__file__).parent


@pytest.fixture(scope="module")
def func() -> Function:
    return PythonFunction.from_callable(check_class)


def test_python_function_serialize(func: Function) -> None:
    json_value = func.model_dump_json(serialize_as_any=True)
    with Path.open(Path(f"{current_dir}/resources/python_function.json")) as f:
        expected_json = f.read()
    actual = json.loads(json_value)
    expected = json.loads(expected_json)
    assert actual == expected


def test_python_function_deserialize(func: Function) -> None:
    with Path.open(Path(f"{current_dir}/resources/python_function.json")) as f:
        expected_json = f.read()
    deserialized_func = PythonFunction.model_validate_json(expected_json)
    assert deserialized_func == func


# Helper function for caching tests
def function_for_caching(value: int) -> int:
    """Test function that returns the input value doubled."""
    return value * 2


# Functions to test selective caching
def generator_function(n: int) -> Generator[int, None, None]:
    """Generator function - should not be cached."""
    yield from range(n)


def function_with_mutable_default(items: list[int] | None = None) -> list[int]:
    """Function with mutable default - should not be cached."""
    if items is None:
        items = []
    items.append(1)
    return items


def make_closure(multiplier: int) -> Callable[[int], int]:
    """Creates a closure - the returned function should not be cached."""

    def inner(value: int) -> int:
        return value * multiplier

    return inner


def simple_pure_function(x: int, y: int) -> int:
    """Simple pure function - should be cached."""
    return x + y


def test_call_python_function_basic() -> None:
    """Test basic functionality of call_python_function."""
    # Clear cache before testing
    clear_python_function_cache()

    result = call_python_function(
        "flink_agents.plan.tests.test_function", "function_for_caching", (5,)
    )
    assert result == 10


def test_call_python_function_caching() -> None:
    """Test that call_python_function reuses cached instances."""
    # Clear cache before testing
    clear_python_function_cache()

    assert get_python_function_cache_size() == 0

    result1 = call_python_function(
        "flink_agents.plan.tests.test_function", "function_for_caching", (3,)
    )
    assert result1 == 6
    assert get_python_function_cache_size() == 1

    result2 = call_python_function(
        "flink_agents.plan.tests.test_function", "function_for_caching", (4,)
    )
    assert result2 == 8
    assert get_python_function_cache_size() == 1  # Cache size should remain 1

    result3 = call_python_function(
        "flink_agents.plan.tests.test_function",
        "check_class",
        (InputEvent(input="test"), OutputEvent(output="test")),
    )
    assert result3 is None
    assert get_python_function_cache_size() == 2


def test_call_python_function_different_modules() -> None:
    """Test caching with different modules."""
    clear_python_function_cache()

    call_python_function(
        "flink_agents.plan.tests.test_function", "function_for_caching", (1,)
    )
    call_python_function(
        "flink_agents.plan.tests.test_function",
        "check_class",
        (InputEvent(input="test"), OutputEvent(output="test")),
    )

    assert get_python_function_cache_size() == 2

    cache_keys = get_python_function_cache_keys()
    expected_keys = [
        ("flink_agents.plan.tests.test_function", "function_for_caching"),
        ("flink_agents.plan.tests.test_function", "check_class"),
    ]
    assert sorted(cache_keys) == sorted(expected_keys)


def test_clear_python_function_cache() -> None:
    """Test clearing the cache."""
    clear_python_function_cache()

    call_python_function(
        "flink_agents.plan.tests.test_function", "function_for_caching", (1,)
    )
    call_python_function(
        "flink_agents.plan.tests.test_function",
        "check_class",
        (InputEvent(input="test"), OutputEvent(output="test")),
    )

    assert get_python_function_cache_size() == 2
    clear_python_function_cache()
    assert get_python_function_cache_size() == 0
    assert get_python_function_cache_keys() == []


def test_cache_performance_benefit() -> None:
    """Test that caching provides performance benefits."""
    clear_python_function_cache()

    # First call creates cache entry
    call_python_function(
        "flink_agents.plan.tests.test_function", "function_for_caching", (1,)
    )
    first_cache_size = get_python_function_cache_size()

    # Multiple subsequent calls should not increase cache size
    for i in range(10):
        call_python_function(
            "flink_agents.plan.tests.test_function", "function_for_caching", (i,)
        )

    assert get_python_function_cache_size() == first_cache_size

    for i in range(5):
        result = call_python_function(
            "flink_agents.plan.tests.test_function", "function_for_caching", (i,)
        )
        assert result == i * 2


def test_selective_caching_pure_functions() -> None:
    """Test that pure functions are cached."""
    clear_python_function_cache()

    call_python_function(
        "flink_agents.plan.tests.test_function", "simple_pure_function", (1, 2)
    )
    call_python_function(
        "flink_agents.plan.tests.test_function", "function_for_caching", (5,)
    )

    assert get_python_function_cache_size() == 2
    cache_keys = get_python_function_cache_keys()
    assert (
        "flink_agents.plan.tests.test_function",
        "simple_pure_function",
    ) in cache_keys
    assert (
        "flink_agents.plan.tests.test_function",
        "function_for_caching",
    ) in cache_keys


def test_selective_caching_generator_functions() -> None:
    """Test that generator functions are not cached."""
    clear_python_function_cache()

    result = call_python_function(
        "flink_agents.plan.tests.test_function", "generator_function", (3,)
    )
    assert isinstance(result, Generator)
    result_list = list(result)
    assert result_list == [0, 1, 2]

    assert get_python_function_cache_size() == 0


def test_selective_caching_mutable_defaults() -> None:
    """Test that functions with mutable defaults are not cached."""
    clear_python_function_cache()

    result1 = call_python_function(
        "flink_agents.plan.tests.test_function", "function_with_mutable_default", ()
    )
    result2 = call_python_function(
        "flink_agents.plan.tests.test_function", "function_with_mutable_default", ()
    )

    assert get_python_function_cache_size() == 0

    assert isinstance(result1, list)
    assert isinstance(result2, list)


def test_is_function_cacheable() -> None:
    """Test the _is_function_cacheable function directly."""
    assert _is_function_cacheable(simple_pure_function) is True
    assert _is_function_cacheable(function_for_caching) is True
    assert _is_function_cacheable(generator_function) is False
    assert _is_function_cacheable(function_with_mutable_default) is False
    closure_func = make_closure(5)
    assert _is_function_cacheable(closure_func) is False
    assert _is_function_cacheable(None) is False


def test_python_function_cacheability_optimization() -> None:
    """Test that PythonFunction caches the cacheability check result."""
    cacheable_func = PythonFunction.from_callable(simple_pure_function)

    assert cacheable_func.is_cacheable() is True
    assert cacheable_func.is_cacheable() is True

    non_cacheable_func = PythonFunction.from_callable(generator_function)

    assert non_cacheable_func.is_cacheable() is False
    assert non_cacheable_func.is_cacheable() is False

    assert cacheable_func.is_cacheable() == _is_function_cacheable(simple_pure_function)
    assert non_cacheable_func.is_cacheable() == _is_function_cacheable(
        generator_function
    )


def test_python_artifact_switch_replaces_the_complete_module_graph(
    tmp_path: Path,
) -> None:
    root = "flink_agents_reload_fixture"
    artifact_v1 = _python_artifact(tmp_path / "v1.zip", root, 100)
    artifact_v2 = _python_artifact(tmp_path / "v2.zip", root, 200)
    original_sys_path = list(sys.path)

    try:
        switch_python_artifact(str(artifact_v1))
        assert call_python_function(f"{root}.agent", "handle", (1,)) == 101
        assert get_python_function_cache_size() == 1
        assert importlib.import_module(f"{root}.utils.data_type").ADDEND == 100
        assert str(artifact_v1) in zipimport._zip_directory_cache

        switch_python_artifact(str(artifact_v2))

        assert root not in sys.modules
        assert f"{root}.agent" not in sys.modules
        assert f"{root}.utils" not in sys.modules
        assert f"{root}.utils.data_type" not in sys.modules
        assert str(artifact_v1) not in sys.path
        assert str(artifact_v2) in sys.path
        assert get_python_function_cache_size() == 0

        assert call_python_function(f"{root}.agent", "handle", (1,)) == 201
        assert importlib.import_module(f"{root}.utils.data_type").ADDEND == 200
    finally:
        switch_python_artifact(None)
        sys.path[:] = original_sys_path
        for loaded_name in list(sys.modules):
            if loaded_name == root or loaded_name.startswith(root + "."):
                sys.modules.pop(loaded_name, None)


def test_python_artifact_roots_come_from_archive_contents(tmp_path: Path) -> None:
    artifact = tmp_path / "agent.whl"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("app/__init__.py", "")
        archive.writestr("app/utils/data_type.py", "")
        archive.writestr("helper.py", "")
        archive.writestr("namespace_pkg/actions/run.py", "")
        archive.writestr("agent-1.0.dist-info/METADATA", "")
        archive.writestr("assets/prompt.txt", "")

    assert _python_artifact_roots(str(artifact)) == (
        "app",
        "helper",
        "namespace_pkg",
    )


def test_switch_purges_old_and_new_artifact_roots(tmp_path: Path) -> None:
    old_root = "flink_agents_union_old_fixture"
    new_root = "flink_agents_union_new_fixture"
    artifact_v1 = tmp_path / "v1.zip"
    artifact_v2 = tmp_path / "v2.zip"
    with zipfile.ZipFile(artifact_v1, "w") as archive:
        archive.writestr(f"{old_root}/__init__.py", "VALUE = 1\n")
        archive.writestr(f"{old_root}/child.py", "VALUE = 2\n")
    with zipfile.ZipFile(artifact_v2, "w") as archive:
        archive.writestr(f"{new_root}/__init__.py", "")

    try:
        switch_python_artifact(str(artifact_v1))
        importlib.import_module(f"{old_root}.child")
        sys.modules[new_root] = types.ModuleType(new_root)

        switch_python_artifact(str(artifact_v2))

        assert old_root not in sys.modules
        assert f"{old_root}.child" not in sys.modules
        assert new_root not in sys.modules
        assert str(artifact_v1) not in sys.path
        assert str(artifact_v2) in sys.path
    finally:
        switch_python_artifact(None)
        for loaded_name in list(sys.modules):
            if loaded_name == old_root or loaded_name.startswith(old_root + "."):
                sys.modules.pop(loaded_name, None)
        sys.modules.pop(new_root, None)


def test_switch_to_no_artifact_retires_old_modules_and_preserves_unrelated_modules(
    tmp_path: Path,
) -> None:
    old_root = "flink_agents_retired_fixture"
    root = "flink_agents_no_artifact_fixture"
    loaded = types.ModuleType(root)
    sys.modules[root] = loaded
    artifact = tmp_path / "agent.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(f"{old_root}/__init__.py", "")

    try:
        switch_python_artifact(str(artifact))
        importlib.import_module(old_root)

        switch_python_artifact(None)

        assert old_root not in sys.modules
        assert str(artifact) not in sys.path
        assert sys.modules[root] is loaded
        switch_python_artifact(None)
        assert sys.modules[root] is loaded
    finally:
        switch_python_artifact(None)
        sys.modules.pop(old_root, None)
        sys.modules.pop(root, None)


def test_python_artifact_reload_only_preserves_framework_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "agent.zip"
    user_root = "flink_agents_user_fixture"
    framework_name = "flink_agents.plan.reload_fixture"
    short_user_names = (
        "function",
        "flink_runner_context",
        "python_java_utils",
    )
    framework_module = types.ModuleType(framework_name)
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("flink_agents/plan/reload_fixture.py", "")
        archive.writestr("function.py", "")
        archive.writestr("flink_runner_context.py", "")
        archive.writestr("python_java_utils.py", "")
        archive.writestr(f"{user_root}/agent.py", "")

    monkeypatch.setitem(sys.modules, framework_name, framework_module)
    for name in short_user_names:
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, user_root, types.ModuleType(user_root))

    try:
        switch_python_artifact(str(artifact))

        assert sys.modules[framework_name] is framework_module
        for name in short_user_names:
            assert name not in sys.modules
        assert user_root not in sys.modules
    finally:
        switch_python_artifact(None)


def test_invalid_artifact_switch_does_not_mutate_active_import_state(
    tmp_path: Path,
) -> None:
    root = "flink_agents_valid_before_invalid_fixture"
    active_artifact = tmp_path / "active.zip"
    with zipfile.ZipFile(active_artifact, "w") as archive:
        archive.writestr(f"{root}/__init__.py", "VALUE = 1\n")
    invalid_artifact = tmp_path / "invalid.zip"
    invalid_artifact.write_text("not a zip archive")

    try:
        switch_python_artifact(str(active_artifact))
        loaded = importlib.import_module(root)
        active_marker = function_module._ACTIVE_PYTHON_ARTIFACT
        active_sys_path = list(sys.path)

        with pytest.raises(zipfile.BadZipFile):
            switch_python_artifact(str(invalid_artifact))

        assert active_marker == function_module._ACTIVE_PYTHON_ARTIFACT
        assert sys.path == active_sys_path
        assert sys.modules[root] is loaded
    finally:
        switch_python_artifact(None)
        sys.modules.pop(root, None)


def test_switch_deduplicates_only_the_artifact_path(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.zip"
    artifact_alias = tmp_path / "artifact-alias.zip"
    runtime = tmp_path / "runtime"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("flink_agents_path_fixture/agent.py", "")
    artifact_alias.symlink_to(artifact)
    runtime.mkdir()
    original_sys_path = list(sys.path)
    importer_cache_entry = str(artifact_alias) + "/flink_agents_path_fixture"

    try:
        sys.path[:0] = [
            str(artifact_alias),
            str(runtime),
            str(artifact),
            str(runtime),
        ]
        sys.path_importer_cache[importer_cache_entry] = object()
        zipimport._zip_directory_cache[str(artifact_alias)] = {}

        switch_python_artifact(str(artifact))

        normalized = [
            str(Path(entry).resolve())
            for entry in sys.path
            if isinstance(entry, str) and entry
        ]
        assert normalized.count(str(artifact.resolve())) == 1
        assert normalized.count(str(runtime.resolve())) == 2
        assert importer_cache_entry not in sys.path_importer_cache
        assert str(artifact_alias) not in zipimport._zip_directory_cache
    finally:
        switch_python_artifact(None)
        sys.path_importer_cache.pop(importer_cache_entry, None)
        zipimport._zip_directory_cache.pop(str(artifact_alias), None)
        sys.path[:] = original_sys_path


def test_switch_does_not_broadcast_import_cache_invalidation(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "artifact.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("flink_agents_targeted_cache_fixture/agent.py", "")

    class UnrelatedFinder:
        @staticmethod
        def find_spec(*args: Any) -> None:
            return None

        @staticmethod
        def invalidate_caches() -> None:
            error_message = "unrelated finder failed"
            raise RuntimeError(error_message)

    finder = UnrelatedFinder()
    sys.meta_path.insert(0, finder)
    try:
        switch_python_artifact(str(artifact))
        assert str(artifact) in sys.path
    finally:
        sys.meta_path.remove(finder)
        switch_python_artifact(None)


def _python_artifact(path: Path, root: str, addend: int) -> Path:
    with zipfile.ZipFile(path, "w") as artifact:
        artifact.writestr(f"{root}/__init__.py", "")
        artifact.writestr(f"{root}/utils/__init__.py", "")
        artifact.writestr(
            f"{root}/utils/data_type.py", f"ADDEND = {addend}\n"
        )
        artifact.writestr(
            f"{root}/agent.py",
            f"from {root}.utils import data_type\n\n"
            "def handle(value):\n"
            "    return value + data_type.ADDEND\n",
        )
    return path

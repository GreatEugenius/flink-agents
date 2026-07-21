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

import asyncio
import importlib
import json
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable
from unittest.mock import Mock

import pytest

from flink_agents.plan import module_namespace


def _artifact(path: Path, value: int) -> Path:
    sources = {
        "app/__init__.py": "",
        "app/utils/__init__.py": "",
        "app/utils/data_type.py": f"VALUE = {value}\n",
        "app/agent.py": (
            "def value():\n"
            "    from app.utils import data_type\n\n"
            "    return data_type.VALUE\n"
        ),
        "app/resources.py": (
            "class VersionedResource:\n"
            "    def __init__(self):\n"
            f"        self.value = {value}\n"
        ),
        "app/tools.py": (
            f'def lookup():\n    """Return the version value."""\n    return {value}\n'
        ),
    }
    with zipfile.ZipFile(path, "w") as artifact:
        for relative_path, source in sources.items():
            artifact.writestr(relative_path, source)
    return path


def _write_artifact(path: Path, sources: dict[str, str]) -> Path:
    with zipfile.ZipFile(path, "w") as artifact:
        for relative_path, source in sources.items():
            artifact.writestr(relative_path, source)
    return path


def _drop(namespace: str) -> None:
    module_namespace.drop(namespace)


def test_plan_versions_keep_independent_absolute_import_graphs(
    tmp_path: Path,
) -> None:
    namespace_v1 = "__fa_job_operator_v1"
    namespace_v2 = "__fa_job_operator_v2"
    artifact_v1 = _artifact(tmp_path / "v1.zip", 100)
    artifact_v2 = _artifact(tmp_path / "v2.zip", 200)
    bare_app_before = sys.modules.get("app")

    try:
        module_namespace.acquire(namespace_v1, artifact_v1)
        module_namespace.acquire(namespace_v2, artifact_v2)
        module_v1 = importlib.import_module(
            module_namespace.qualify(namespace_v1, "app.agent")
        )
        module_v2 = importlib.import_module(
            module_namespace.qualify(namespace_v2, "app.agent")
        )

        assert module_v1.value() == 100
        assert module_v2.value() == 200
        assert module_v1 is not module_v2
        assert sys.modules.get("app") is bare_app_before
        assert (
            sys.modules[f"{namespace_v1}.app.utils.data_type"]
            is not sys.modules[f"{namespace_v2}.app.utils.data_type"]
        )
    finally:
        _drop(namespace_v1)
        _drop(namespace_v2)


def test_shared_version_is_dropped_only_after_last_runtime_releases_it(
    tmp_path: Path,
) -> None:
    namespace = "__fa_job_operator_v3"
    artifact = _artifact(tmp_path / "v3.zip", 300)

    try:
        module_namespace.acquire(namespace, artifact)
        module_namespace.acquire(namespace, artifact)
        importlib.import_module(module_namespace.qualify(namespace, "app.agent"))

        assert module_namespace.usage_count(namespace) == 2
        assert module_namespace.release(namespace, drop_if_unused=True) is False
        assert namespace in sys.modules
        assert module_namespace.release(namespace, drop_if_unused=True) is True
        assert namespace not in sys.modules
    finally:
        _drop(namespace)


def test_namespace_cannot_be_reused_for_another_artifact(tmp_path: Path) -> None:
    namespace = "__fa_job_operator_v4"
    artifact_v1 = _artifact(tmp_path / "v4-a.zip", 400)
    artifact_v2 = _artifact(tmp_path / "v4-b.zip", 401)

    try:
        module_namespace.acquire(namespace, artifact_v1)
        with pytest.raises(ValueError, match="already registered"):
            module_namespace.acquire(namespace, artifact_v2)
    finally:
        _drop(namespace)


def test_runtime_resource_and_tool_entrypoints_use_plan_namespace(
    tmp_path: Path,
) -> None:
    from flink_agents.runtime.python_java_utils import (
        create_resource,
        invoke_python_tool,
    )

    namespace_v1 = "__fa_job_operator_v5"
    namespace_v2 = "__fa_job_operator_v6"
    artifact_v1 = _artifact(tmp_path / "v5.zip", 500)
    artifact_v2 = _artifact(tmp_path / "v6.zip", 600)

    try:
        module_namespace.acquire(namespace_v1, artifact_v1)
        module_namespace.acquire(namespace_v2, artifact_v2)

        resource_v1 = create_resource(
            "app.resources", "VersionedResource", {}, namespace_v1
        )
        resource_v2 = create_resource(
            "app.resources", "VersionedResource", {}, namespace_v2
        )

        assert resource_v1.value == 500
        assert resource_v2.value == 600
        assert invoke_python_tool("app.tools", "lookup", {}, namespace_v1) == 500
        assert invoke_python_tool("app.tools", "lookup", {}, namespace_v2) == 600
    finally:
        _drop(namespace_v1)
        _drop(namespace_v2)


@pytest.mark.parametrize(
    ("import_source", "value_expression", "utils_init"),
    [
        (
            "import app.utils.data_type\n",
            "app.utils.data_type.VALUE",
            "",
        ),
        (
            "import app.utils.data_type as data_type\n",
            "data_type.VALUE",
            "",
        ),
        (
            "from app.utils import data_type\n",
            "data_type.VALUE",
            "",
        ),
        (
            "from app.utils.data_type import VALUE\n",
            "VALUE",
            "",
        ),
        (
            "from .utils import data_type\n",
            "data_type.VALUE",
            "",
        ),
        (
            "from .utils.data_type import VALUE\n",
            "VALUE",
            "",
        ),
        (
            "from app.utils import VALUE\n",
            "VALUE",
            "from .data_type import VALUE\n",
        ),
    ],
    ids=[
        "absolute-import",
        "absolute-import-alias",
        "absolute-from-package",
        "absolute-from-module",
        "relative-from-package",
        "relative-from-module",
        "package-reexport",
    ],
)
def test_common_import_forms_resolve_each_plan_dependency_version(
    tmp_path: Path,
    import_source: str,
    value_expression: str,
    utils_init: str,
) -> None:
    namespace_v1 = "__fa_import_forms_v1"
    namespace_v2 = "__fa_import_forms_v2"

    def import_artifact(path: Path, value: int) -> Path:
        return _write_artifact(
            path,
            {
                "app/__init__.py": "",
                "app/utils/__init__.py": utils_init,
                "app/utils/data_type.py": f"VALUE = {value}\n",
                "app/agent.py": (
                    import_source
                    + "\n"
                    + "def value():\n"
                    + f"    return {value_expression}\n"
                ),
            },
        )

    try:
        module_namespace.acquire(namespace_v1, import_artifact(tmp_path / "v1.zip", 10))
        module_namespace.acquire(namespace_v2, import_artifact(tmp_path / "v2.zip", 20))

        action_v1 = importlib.import_module(f"{namespace_v1}.app.agent")
        action_v2 = importlib.import_module(f"{namespace_v2}.app.agent")

        assert action_v1.value() == 10
        assert action_v2.value() == 20
    finally:
        _drop(namespace_v1)
        _drop(namespace_v2)


def test_action_and_tool_changes_use_their_own_transitive_dependencies(
    tmp_path: Path,
) -> None:
    from flink_agents.runtime.python_java_utils import (
        get_python_tool_metadata,
        invoke_python_tool,
    )

    namespace_v1 = "__fa_action_tool_v1"
    namespace_v2 = "__fa_action_tool_v2"

    def action_tool_artifact(path: Path, version: str, offset: int) -> Path:
        extra_parameter = "" if version == "v1" else ", scale: int = 2"
        calculation = "value + OFFSET" if version == "v1" else "value * scale + OFFSET"
        action_arguments = "value" if version == "v1" else "value, scale=3"
        return _write_artifact(
            path,
            {
                "app/__init__.py": "",
                "app/shared.py": f"OFFSET = {offset}\n",
                "app/tools.py": (
                    "from app.shared import OFFSET\n\n"
                    + f"def lookup(value: int{extra_parameter}) -> str:\n"
                    + f'    """Lookup implemented by {version}."""\n'
                    + f'    return f"tool-{version}:{{{calculation}}}"\n'
                ),
                "app/action.py": (
                    "from app.tools import lookup\n\n"
                    + "def handle(value: int) -> str:\n"
                    + f'    return "action-{version}:" + lookup({action_arguments})\n'
                ),
            },
        )

    try:
        module_namespace.acquire(
            namespace_v1, action_tool_artifact(tmp_path / "v1.zip", "v1", 1)
        )
        module_namespace.acquire(
            namespace_v2, action_tool_artifact(tmp_path / "v2.zip", "v2", 2)
        )

        action_v1 = importlib.import_module(f"{namespace_v1}.app.action")
        action_v2 = importlib.import_module(f"{namespace_v2}.app.action")
        metadata_v1 = get_python_tool_metadata(
            "app.tools", "lookup", namespace=namespace_v1
        )
        metadata_v2 = get_python_tool_metadata(
            "app.tools", "lookup", namespace=namespace_v2
        )

        assert action_v1.handle(4) == "action-v1:tool-v1:5"
        assert action_v2.handle(4) == "action-v2:tool-v2:14"
        assert metadata_v1["description"] == "Lookup implemented by v1."
        assert metadata_v2["description"] == "Lookup implemented by v2."
        assert "scale" not in metadata_v1["inputSchema"]
        assert "scale" in metadata_v2["inputSchema"]
        assert (
            invoke_python_tool("app.tools", "lookup", {"value": 5}, namespace_v1)
            == "tool-v1:6"
        )
        assert (
            invoke_python_tool(
                "app.tools", "lookup", {"value": 5, "scale": 4}, namespace_v2
            )
            == "tool-v2:22"
        )
    finally:
        _drop(namespace_v1)
        _drop(namespace_v2)


def test_resource_class_change_uses_the_matching_dependency_version(
    tmp_path: Path,
) -> None:
    from flink_agents.runtime.python_java_utils import create_resource

    namespace_v1 = "__fa_resource_dependency_v1"
    namespace_v2 = "__fa_resource_dependency_v2"

    def resource_artifact(path: Path, value: str) -> Path:
        return _write_artifact(
            path,
            {
                "app/__init__.py": "",
                "app/settings.py": f'VALUE = "{value}"\n',
                "app/resources.py": (
                    "from app.settings import VALUE\n\n"
                    "class VersionedResource:\n"
                    "    def __init__(self):\n"
                    "        self.value = VALUE\n"
                ),
            },
        )

    try:
        module_namespace.acquire(
            namespace_v1, resource_artifact(tmp_path / "v1.zip", "resource-v1")
        )
        module_namespace.acquire(
            namespace_v2, resource_artifact(tmp_path / "v2.zip", "resource-v2")
        )

        resource_v1 = create_resource(
            "app.resources", "VersionedResource", {}, namespace_v1
        )
        resource_v2 = create_resource(
            "app.resources", "VersionedResource", {}, namespace_v2
        )

        assert resource_v1.value == "resource-v1"
        assert resource_v2.value == "resource-v2"
    finally:
        _drop(namespace_v1)
        _drop(namespace_v2)


def test_added_and_removed_modules_do_not_leak_between_plan_versions(
    tmp_path: Path,
) -> None:
    namespace_v1 = "__fa_module_set_v1"
    namespace_v2 = "__fa_module_set_v2"
    artifact_v1 = _write_artifact(
        tmp_path / "v1.zip",
        {
            "app/__init__.py": "",
            "app/legacy.py": 'VALUE = "legacy"\n',
            "app/agent.py": "from app.legacy import VALUE\n",
        },
    )
    artifact_v2 = _write_artifact(
        tmp_path / "v2.zip",
        {
            "app/__init__.py": "",
            "app/fresh.py": 'VALUE = "fresh"\n',
            "app/agent.py": "from app.fresh import VALUE\n",
        },
    )

    try:
        module_namespace.acquire(namespace_v1, artifact_v1)
        module_namespace.acquire(namespace_v2, artifact_v2)
        action_v1 = importlib.import_module(f"{namespace_v1}.app.agent")
        action_v2 = importlib.import_module(f"{namespace_v2}.app.agent")

        assert action_v1.VALUE == "legacy"
        assert action_v2.VALUE == "fresh"
        assert f"{namespace_v1}.app.legacy" in sys.modules
        assert f"{namespace_v2}.app.legacy" not in sys.modules
        assert f"{namespace_v2}.app.fresh" in sys.modules
        assert f"{namespace_v1}.app.fresh" not in sys.modules

        module_namespace.drop(namespace_v1)

        assert action_v2.VALUE == "fresh"
        assert f"{namespace_v2}.app.fresh" in sys.modules
    finally:
        _drop(namespace_v1)
        _drop(namespace_v2)


def test_suspended_old_coroutine_keeps_its_dependency_graph_after_v2_loads(
    tmp_path: Path,
) -> None:
    namespace_v1 = "__fa_async_reference_v1"
    namespace_v2 = "__fa_async_reference_v2"

    def async_artifact(path: Path, value: str) -> Path:
        return _write_artifact(
            path,
            {
                "app/__init__.py": "",
                "app/value.py": f'VALUE = "{value}"\n',
                "app/action.py": (
                    "import asyncio\n"
                    "from app.value import VALUE\n\n"
                    "async def handle():\n"
                    "    await asyncio.sleep(0)\n"
                    "    return VALUE\n"
                ),
            },
        )

    async def exercise() -> tuple[str, str]:
        module_namespace.acquire(
            namespace_v1, async_artifact(tmp_path / "v1.zip", "async-v1")
        )
        action_v1 = importlib.import_module(f"{namespace_v1}.app.action")
        suspended_v1 = asyncio.create_task(action_v1.handle())
        await asyncio.sleep(0)

        module_namespace.acquire(
            namespace_v2, async_artifact(tmp_path / "v2.zip", "async-v2")
        )
        action_v2 = importlib.import_module(f"{namespace_v2}.app.action")
        return await suspended_v1, await action_v2.handle()

    try:
        assert asyncio.run(exercise()) == ("async-v1", "async-v2")
    finally:
        _drop(namespace_v1)
        _drop(namespace_v2)


@pytest.mark.parametrize(
    "tool_call_async", [False, True], ids=["sync-tool-call", "async-tool-call"]
)
def test_builtin_tool_call_action_uses_rebuilt_context_plan_namespace(
    tmp_path: Path,
    tool_call_async: bool,
) -> None:
    from flink_agents.api.events.tool_event import ToolRequestEvent, ToolResponseEvent
    from flink_agents.plan.actions.tool_call_action import process_tool_request
    from flink_agents.runtime.flink_runner_context import FlinkRunnerContext

    namespace_v1 = "__fa_builtin_tool_v1"
    namespace_v2 = "__fa_builtin_tool_v2"

    def tool_artifact(path: Path, version: str, offset: int) -> Path:
        return _write_artifact(
            path,
            {
                "app/__init__.py": "",
                "app/shared.py": f"OFFSET = {offset}\n",
                "app/tools.py": (
                    "from app.shared import OFFSET\n\n"
                    "def lookup(value: int) -> str:\n"
                    f'    """Lookup implemented by {version}."""\n'
                    f'    return f"{version}:{{value + OFFSET}}"\n'
                ),
            },
        )

    plan_json = json.dumps(
        {
            "actions": {},
            "actions_by_event": {},
            "resource_providers": {
                "tool": {
                    "lookup": {
                        "name": "lookup",
                        "type": "tool",
                        "module": "flink_agents.plan.tools.function_tool",
                        "clazz": "FunctionTool",
                        "serialized": {
                            "name": "lookup",
                            "func": {
                                "func_type": "PythonFunction",
                                "module": "app.tools",
                                "qualname": "lookup",
                            },
                            "injected_args": {},
                            "metadata": None,
                        },
                        "__resource_provider_type__": (
                            "PythonSerializableResourceProvider"
                        ),
                    }
                }
            },
            "config": {"conf_data": {"tool-call.async": tool_call_async}},
        }
    )

    async def invoke(context: FlinkRunnerContext) -> str:
        async def execute_async(func: Callable[..., Any], **kwargs: Any) -> Any:
            return func(**kwargs)

        sent_events = []
        resource_cache = context._FlinkRunnerContext__resource_cache
        context.get_resource = resource_cache.get_resource
        context.durable_execute = lambda func, **kwargs: func(**kwargs)
        context.durable_execute_async = execute_async
        context.send_event = sent_events.append
        request = ToolRequestEvent(
            model="test-model",
            tool_calls=[
                {
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "lookup", "arguments": {"value": 5}},
                }
            ],
        )

        await process_tool_request(request, context)

        response = ToolResponseEvent.from_event(sent_events[0])
        assert response.success["call-1"] is True, response.error
        return response.responses["call-1"]

    executor = ThreadPoolExecutor(max_workers=1)
    context_v1 = None
    context_v2 = None
    try:
        module_namespace.acquire(
            namespace_v1, tool_artifact(tmp_path / "v1.zip", "tool-v1", 1)
        )
        context_v1 = FlinkRunnerContext(
            Mock(), plan_json, executor, Mock(), namespace_v1
        )
        assert asyncio.run(invoke(context_v1)) == "tool-v1:6"

        module_namespace.acquire(
            namespace_v2, tool_artifact(tmp_path / "v2.zip", "tool-v2", 2)
        )
        context_v2 = FlinkRunnerContext(
            Mock(), plan_json, executor, Mock(), namespace_v2
        )
        assert asyncio.run(invoke(context_v2)) == "tool-v2:7"
    finally:
        if context_v1 is not None:
            context_v1.close()
        if context_v2 is not None:
            context_v2.close()
        executor.shutdown(wait=True)
        _drop(namespace_v1)
        _drop(namespace_v2)

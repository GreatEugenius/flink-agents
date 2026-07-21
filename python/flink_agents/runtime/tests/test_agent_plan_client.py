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
from unittest.mock import MagicMock, patch

import pytest

from flink_agents.api.agent_plan_client import AgentPlanClient, PlanUpdateResult
from flink_agents.api.agents.agent import Agent
from flink_agents.runtime.agent_plan_client import (
    RemoteAgentPlanClient,
    create_instance,
)


def test_factory_loads_version_jar_before_creating_runtime_client() -> None:
    """The independent client bootstraps its Java classes without an execution env."""
    expected = MagicMock()
    runtime_module = MagicMock()
    runtime_module.create_instance.return_value = expected

    with (
        patch(
            "flink_agents.api.agent_plan_client.add_flink_agents_jars_to_context_class_loader"
        ) as add_jars,
        patch(
            "flink_agents.api.agent_plan_client.importlib.import_module",
            return_value=runtime_module,
        ),
    ):
        actual = AgentPlanClient.create("localhost", 8081)

    assert actual is expected
    add_jars.assert_called_once_with()
    runtime_module.create_instance.assert_called_once_with("localhost", 8081)


def test_runtime_factory_invokes_boxed_integer_java_bridge() -> None:
    """The PyFlink reflection helper resolves the boxed Integer type."""
    java_client = MagicMock()
    java_port = MagicMock()
    gateway = MagicMock()
    gateway.jvm.java.lang.Integer.return_value = java_port

    with (
        patch(
            "flink_agents.runtime.agent_plan_client.invoke_method",
            return_value=java_client,
        ) as invoke,
        patch(
            "flink_agents.runtime.agent_plan_client.get_gateway",
            return_value=gateway,
        ),
    ):
        actual = create_instance("localhost", 8081)

    assert isinstance(actual, RemoteAgentPlanClient)
    assert invoke.call_args.args[2] == "create"
    assert invoke.call_args.args[3] == ["localhost", java_port]
    assert invoke.call_args.args[4] == ["java.lang.String", "java.lang.Integer"]


@pytest.mark.parametrize(
    ("rest_address", "rest_port"),
    [("", 8081), (" ", 8081), ("localhost", 0), ("localhost", 65536)],
)
def test_factory_rejects_invalid_endpoint(rest_address: str, rest_port: int) -> None:
    with pytest.raises(ValueError, match="REST"):
        AgentPlanClient.create(rest_address, rest_port)


def test_update_plan_json_invokes_java_bridge_and_converts_result() -> None:
    """Python sends strings only and exposes an immutable Python result."""
    java_client = MagicMock()
    java_result = MagicMock()
    java_result.getPlanVersion.return_value = 4
    java_result.getPlanId.return_value = "plan-id"
    java_result.getRegisteredSubtasks.return_value = 2
    client = RemoteAgentPlanClient(java_client)

    with patch(
        "flink_agents.runtime.agent_plan_client.invoke_method",
        return_value=java_result,
    ) as invoke:
        result = client.update_agent_plan("0" * 32, "review-agent", '{"actions":{}}')

    assert result == PlanUpdateResult(
        plan_version=4, plan_id="plan-id", registered_subtasks=2
    )
    assert invoke.call_args.args[2] == "updateAgentPlan"
    assert invoke.call_args.args[3] == [
        "0" * 32,
        "review-agent",
        '{"actions":{}}',
    ]
    assert invoke.call_args.args[4] == [
        "java.lang.String",
        "java.lang.String",
        "java.lang.String",
    ]


def test_update_agent_builds_plan_and_defaults_to_class_name() -> None:
    """The convenience API reuses Agent-to-AgentPlan compilation."""
    client = RemoteAgentPlanClient(MagicMock())
    expected = PlanUpdateResult(2, "plan-id", 1)

    with patch.object(
        client, "update_agent_plan", return_value=expected
    ) as update_plan:
        actual = client.update_agent("0" * 32, NamedAgent())

    assert actual == expected
    assert update_plan.call_args.args[1] == "NamedAgent"
    assert '"actions"' in update_plan.call_args.args[2]


def test_update_agent_accepts_explicit_deployment_name() -> None:
    """Updates can target a stable name after the Agent class changes."""
    client = RemoteAgentPlanClient(MagicMock())
    expected = PlanUpdateResult(2, "plan-id", 1)

    with patch.object(
        client, "update_agent_plan", return_value=expected
    ) as update_plan:
        client.update_agent("0" * 32, NamedAgent(), name="review-agent")

    assert update_plan.call_args.args[1] == "review-agent"


def test_update_agent_builds_plan_with_explicit_config_mapping() -> None:
    """The independent client accepts config data without an execution env."""
    client = RemoteAgentPlanClient(MagicMock())
    expected = PlanUpdateResult(2, "plan-id", 1)

    with patch.object(
        client, "update_agent_plan", return_value=expected
    ) as update_plan:
        client.update_agent(
            "0" * 32,
            NamedAgent(),
            config={"dynamic-plan.test-key": "test-value"},
        )

    assert '"dynamic-plan.test-key":"test-value"' in update_plan.call_args.args[2]


def test_client_context_manager_closes_once() -> None:
    """The wrapper owns and closes its Java RestClusterClient."""
    java_client = MagicMock()
    client = RemoteAgentPlanClient(java_client)

    with patch("flink_agents.runtime.agent_plan_client.invoke_method") as invoke:
        with client as entered:
            assert entered is client
        client.close()

    close_calls = [call for call in invoke.call_args_list if call.args[2] == "close"]
    assert len(close_calls) == 1


def test_update_after_close_is_rejected() -> None:
    """A closed wrapper never invokes the Java client again."""
    client = RemoteAgentPlanClient(MagicMock())

    with patch("flink_agents.runtime.agent_plan_client.invoke_method"):
        client.close()

    with pytest.raises(RuntimeError, match="closed"):
        client.update_agent_plan("0" * 32, "review-agent", "{}")


def test_close_failure_still_marks_client_closed() -> None:
    """A partially closed Java client is never invoked a second time."""
    client = RemoteAgentPlanClient(MagicMock())

    with patch(
        "flink_agents.runtime.agent_plan_client.invoke_method",
        side_effect=RuntimeError("close failed"),
    ) as invoke:
        with pytest.raises(RuntimeError, match="close failed"):
            client.close()
        client.close()

    invoke.assert_called_once()


class NamedAgent(Agent):
    """Agent with a deterministic deployment name."""

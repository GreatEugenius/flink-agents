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
from types import TracebackType
from typing import Any, Mapping, Type

from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import invoke_method
from typing_extensions import Self

from flink_agents.api.agent_plan_client import AgentPlanClient, PlanUpdateResult
from flink_agents.api.agents.agent import Agent
from flink_agents.plan.agent_plan import AgentPlan
from flink_agents.plan.configuration import AgentConfiguration

_JAVA_CLIENT_CLASS = "org.apache.flink.agents.runtime.client.AgentPlanClient"
_JAVA_INTEGER = "java.lang.Integer"
_JAVA_STRING = "java.lang.String"


class RemoteAgentPlanClient(AgentPlanClient):
    """AgentPlan client backed by Flink's Java RestClusterClient."""

    def __init__(self, java_client: object) -> None:
        """Wrap an initialized Java AgentPlanClient."""
        self._java_client = java_client
        self._closed = False

    def update_agent_plan(
        self, job_id: str, agent_name: str, agent_plan_json: str
    ) -> PlanUpdateResult:
        """Validate and stage a complete AgentPlan JSON document."""
        self._ensure_open()
        _validate_agent_name(agent_name)
        java_result = invoke_method(
            self._java_client,
            _JAVA_CLIENT_CLASS,
            "updateAgentPlan",
            [job_id, agent_name, agent_plan_json],
            [_JAVA_STRING, _JAVA_STRING, _JAVA_STRING],
        )
        return PlanUpdateResult(
            plan_version=java_result.getPlanVersion(),
            plan_id=java_result.getPlanId(),
            registered_subtasks=java_result.getRegisteredSubtasks(),
        )

    def update_agent(
        self,
        job_id: str,
        agent: Agent,
        name: str | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> PlanUpdateResult:
        """Compile an Agent with optional config data and stage it."""
        agent_name = name if name is not None else agent.__class__.__name__
        _validate_agent_name(agent_name)
        agent_config = AgentConfiguration(dict(config) if config is not None else None)
        plan = AgentPlan.from_agent(agent, agent_config)
        return self.update_agent_plan(
            job_id,
            agent_name,
            plan.model_dump_json(serialize_as_any=True),
        )

    def close(self) -> None:
        """Close the Java REST client once."""
        if self._closed:
            return
        self._closed = True
        invoke_method(self._java_client, _JAVA_CLIENT_CLASS, "close", [], [])

    def __enter__(self) -> Self:
        """Return this client for use as a context manager."""
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close this client when leaving a context manager."""
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "AgentPlanClient is already closed."
            raise RuntimeError(msg)


def create_instance(rest_address: str, rest_port: int) -> AgentPlanClient:
    """Create a Python wrapper around the Java AgentPlanClient."""
    java_port = get_gateway().jvm.java.lang.Integer(rest_port)
    java_client = invoke_method(
        None,
        _JAVA_CLIENT_CLASS,
        "create",
        [rest_address, java_port],
        [_JAVA_STRING, _JAVA_INTEGER],
    )
    return RemoteAgentPlanClient(java_client)


def _validate_agent_name(agent_name: str) -> None:
    if not agent_name or not agent_name.strip():
        msg = "The agent name must not be blank."
        raise ValueError(msg)

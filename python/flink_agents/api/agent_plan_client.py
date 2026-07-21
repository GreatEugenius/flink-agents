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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Mapping, Type

from typing_extensions import Self

from flink_agents.api._java_utils import (
    add_flink_agents_jars_to_context_class_loader,
)

if TYPE_CHECKING:
    from flink_agents.api.agents.agent import Agent


@dataclass(frozen=True)
class PlanUpdateResult:
    """Confirmation that the current JobManager attempt accepted an AgentPlan.

    ``registered_subtasks`` is the number of subtask gateways registered when
    the request was accepted as a volatile candidate. It does not mean the plan
    was announced, made durable, or became effective.
    """

    plan_version: int
    plan_id: str
    registered_subtasks: int


class AgentPlanClient(ABC):
    """Client for submitting AgentPlan updates to a running Flink job."""

    @staticmethod
    def create(rest_address: str, rest_port: int) -> "AgentPlanClient":
        """Create a client for a Flink JobManager REST endpoint.

        Args:
            rest_address: JobManager hostname or IP address, without a URL scheme
                or path.
            rest_port: JobManager REST port in the range 1 through 65535.

        Raises:
            ValueError: If the address is blank or the port is outside the valid
                range.
        """
        if not rest_address or not rest_address.strip():
            msg = "The REST address must not be blank."
            raise ValueError(msg)
        if rest_port <= 0 or rest_port > 65535:
            msg = "The REST port must be between 1 and 65535."
            raise ValueError(msg)
        add_flink_agents_jars_to_context_class_loader()
        runtime_module = importlib.import_module(
            "flink_agents.runtime.agent_plan_client"
        )
        return runtime_module.create_instance(rest_address, rest_port)

    @abstractmethod
    def update_agent_plan(
        self, job_id: str, agent_name: str, agent_plan_json: str
    ) -> PlanUpdateResult:
        """Validate and submit a complete AgentPlan JSON document."""

    @abstractmethod
    def update_agent(
        self,
        job_id: str,
        agent: "Agent",
        name: str | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> PlanUpdateResult:
        """Compile an Agent with optional config data and submit it."""

    @abstractmethod
    def close(self) -> None:
        """Close the underlying REST client."""

    def __enter__(self) -> Self:
        """Return this client for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close this client when leaving a context manager."""
        self.close()

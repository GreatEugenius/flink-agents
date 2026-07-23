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
import time
from contextlib import suppress
from pathlib import Path

from pyflink.common import Configuration, Duration, JobStatus, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.java_gateway import get_gateway

from flink_agents.api.agent_plan_client import AgentPlanClient
from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.execution_environment import AgentsExecutionEnvironment
from flink_agents.api.runner_context import RunnerContext

_AGENT_NAME = "review-agent"


class V1Agent(Agent):
    """Initial plan used by the running job."""

    @action(InputEvent.EVENT_TYPE)
    @staticmethod
    def on_input(event: Event, context: RunnerContext) -> None:
        value = InputEvent.from_event(event).input
        context.send_event(OutputEvent(output=f"v1:{value}"))


class V2Agent(Agent):
    """Replacement plan submitted through the Python client."""

    @action(InputEvent.EVENT_TYPE)
    @staticmethod
    def on_input(event: Event, context: RunnerContext) -> None:
        value = InputEvent.from_event(event).input
        context.send_event(OutputEvent(output=f"v2:{value}"))


class _AppendToFile:
    def __init__(self, path: Path) -> None:
        self._path = path

    def __call__(self, value: str) -> str:
        with self._path.open("a", encoding="utf-8") as output:
            output.write(f"{value}\n")
        return value


def test_python_client_updates_running_job_at_second_checkpoint(
    tmp_path: Path,
) -> None:
    """Start a real job and switch its Python AgentPlan through JM REST."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    result_file = tmp_path / "results.txt"
    _add_input(input_dir, 1)

    configuration = Configuration()
    configuration.set_string("rest.bind-port", "0")
    configuration.set_integer("taskmanager.numberOfTaskSlots", 1)
    configuration.set_string("restart-strategy.type", "disable")
    configuration.set_integer("python.fn-execution.bundle.size", 1)

    gateway = get_gateway()
    cluster = _start_mini_cluster(gateway, configuration)
    job_client = None
    try:
        rest_uri = cluster.getRestAddress().get(
            30,
            gateway.jvm.java.util.concurrent.TimeUnit.SECONDS,
        )
        rest_address = str(rest_uri.getHost())
        rest_port = int(rest_uri.getPort())
        env = _create_remote_environment(
            gateway,
            configuration,
            rest_address,
            rest_port,
        )
        _build_job(env, input_dir, result_file)
        job_client = env.execute_async("python-agent-plan-client-e2e")
        _wait_until_running(job_client)
        job_id = str(job_client.get_job_id())

        _wait_for_lines(job_client, result_file, ["v1:1"])

        with AgentPlanClient.create(rest_address, rest_port) as client:
            staged = client.update_agent(job_id, V2Agent(), name=_AGENT_NAME)
            assert staged.plan_version == 1
            assert staged.registered_subtasks == 1

        _add_input(input_dir, 2)
        _wait_for_lines(job_client, result_file, ["v1:1", "v1:2"])

        _trigger_checkpoint(gateway, cluster, job_id)
        _add_input(input_dir, 3)
        _wait_for_lines(job_client, result_file, ["v1:1", "v1:2", "v1:3"])

        _trigger_checkpoint(gateway, cluster, job_id)
        _add_input(input_dir, 4)
        _wait_for_lines(job_client, result_file, ["v1:1", "v1:2", "v1:3", "v2:4"])

        assert _read_lines(result_file) == ["v1:1", "v1:2", "v1:3", "v2:4"]
        _assert_running(job_client)
    finally:
        with suppress(Exception):
            if (
                job_client is not None
                and not job_client.get_job_status().result().is_terminal_state()
            ):
                job_client.cancel().result(timeout=30)
        cluster.closeAsync().get(
            30,
            gateway.jvm.java.util.concurrent.TimeUnit.SECONDS,
        )


def _start_mini_cluster(gateway: object, configuration: Configuration) -> object:
    cluster_configuration = (
        gateway.jvm.org.apache.flink.runtime.minicluster.MiniClusterConfiguration.Builder()
        .setConfiguration(configuration._j_configuration)
        .setNumTaskManagers(2)
        .setNumSlotsPerTaskManager(1)
        .withRandomPorts()
        .build()
    )
    cluster = gateway.jvm.org.apache.flink.runtime.minicluster.MiniCluster(
        cluster_configuration
    )
    cluster.start()
    return cluster


def _create_remote_environment(
    gateway: object,
    configuration: Configuration,
    rest_address: str,
    rest_port: int,
) -> StreamExecutionEnvironment:
    empty_jars = gateway.new_array(gateway.jvm.java.lang.String, 0)
    java_env = gateway.jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createRemoteEnvironment(
        rest_address,
        rest_port,
        configuration._j_configuration,
        empty_jars,
    )
    return StreamExecutionEnvironment(java_env)


def _build_job(
    env: StreamExecutionEnvironment,
    input_dir: Path,
    result_file: Path,
) -> None:
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(86_400_000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    source = (
        FileSource.for_record_stream_format(
            StreamFormat.text_line_format(), input_dir.as_uri()
        )
        .monitor_continuously(Duration.of_millis(100))
        .build()
    )
    input_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "agent-plan-update-input",
    )
    python_input = input_stream.map(lambda value: value)
    agents = AgentsExecutionEnvironment.get_execution_environment(env=env)
    output_stream = (
        agents.from_datastream(python_input, key_selector=lambda value: value)
        .apply(V1Agent(), name=_AGENT_NAME)
        .to_datastream(output_type=Types.STRING())
    )
    output_stream.map(_AppendToFile(result_file), output_type=Types.STRING()).print()


def _add_input(input_dir: Path, value: int) -> None:
    temporary = input_dir / f".{value}.tmp"
    temporary.write_text(str(value), encoding="utf-8")
    temporary.replace(input_dir / f"{value}.txt")


def _wait_until_running(job_client: object) -> None:
    deadline = time.monotonic() + 30
    status = job_client.get_job_status().result()
    while status != JobStatus.RUNNING and time.monotonic() < deadline:
        time.sleep(0.05)
        try:
            status = job_client.get_job_status().result()
        except Exception as error:
            result = job_client.get_job_execution_result().result()
            message = f"MiniCluster stopped before the job reached RUNNING: {result!r}"
            raise AssertionError(message) from error
    assert status == JobStatus.RUNNING


def _assert_running(job_client: object) -> None:
    status = job_client.get_job_status().result()
    if status.is_terminal_state():
        job_client.get_job_execution_result().result()
    assert status == JobStatus.RUNNING


def _wait_for_lines(job_client: object, result_file: Path, expected: list[str]) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if _read_lines(result_file) == expected:
            return
        try:
            status = job_client.get_job_status().result()
        except Exception as error:
            result = job_client.get_job_execution_result().result()
            message = f"MiniCluster stopped before producing {expected!r}: {result!r}"
            raise AssertionError(message) from error
        if status.is_terminal_state():
            job_client.get_job_execution_result().result()
            message = f"Job reached {status.name} before producing {expected!r}."
            raise AssertionError(message)
        time.sleep(0.05)
    message = f"Timed out waiting for {expected!r}; actual={_read_lines(result_file)!r}"
    raise AssertionError(message)


def _read_lines(result_file: Path) -> list[str]:
    if not result_file.exists():
        return []
    return result_file.read_text(encoding="utf-8").splitlines()


def _trigger_checkpoint(gateway: object, cluster: object, job_id: str) -> None:
    java_job_id = gateway.jvm.org.apache.flink.api.common.JobID.fromHexString(job_id)
    checkpoint_type = (
        gateway.jvm.org.apache.flink.core.execution.CheckpointType.CONFIGURED
    )
    cluster.triggerCheckpoint(java_job_id, checkpoint_type).get(
        30,
        gateway.jvm.java.util.concurrent.TimeUnit.SECONDS,
    )

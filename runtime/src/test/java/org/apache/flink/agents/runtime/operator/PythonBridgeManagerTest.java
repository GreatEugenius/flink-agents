/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.runtime.operator;

import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.plan.AgentConfiguration;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.PythonFunction;
import org.apache.flink.agents.plan.actions.Action;
import org.apache.flink.agents.runtime.operator.coordinator.PlanIds;
import org.apache.flink.agents.runtime.python.utils.PythonActionExecutor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pemja.core.PythonInterpreter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Contract tests for {@link PythonBridgeManager}. */
class PythonBridgeManagerTest {

    @Test
    void pythonArtifactMetadataIsValidatedWithoutStartingCPython(@TempDir Path tempDir)
            throws Exception {
        Path artifact = pythonArtifact(tempDir.resolve("agent.zip"));

        try (PythonBridgeManager bridge = new PythonBridgeManager()) {
            bridge.validatePythonPlanMetadata(pythonPlan(artifact));

            assertThat(bridge.isInitialized()).isFalse();
        }
    }

    @Test
    void invalidPythonArtifactDigestIsRejectedBeforeStartingCPython(@TempDir Path tempDir)
            throws Exception {
        Path artifact = pythonArtifact(tempDir.resolve("agent.zip"));
        AgentPlan plan = pythonPlan(artifact);
        plan.getConfig().setStr(PythonBridgeManager.PYTHON_ARTIFACT_SHA256_CONFIG, "0".repeat(64));

        try (PythonBridgeManager bridge = new PythonBridgeManager()) {
            assertThatThrownBy(() -> bridge.validatePythonPlanMetadata(plan))
                    .hasMessageContaining("failed sha256 verification");
            assertThat(bridge.isInitialized()).isFalse();
        }
    }

    @Test
    void pythonArtifactPathAndDigestMustBeConfiguredTogether(@TempDir Path tempDir)
            throws Exception {
        Path artifact = pythonArtifact(tempDir.resolve("agent.zip"));
        AgentPlan pathOnly = pythonPlan(artifact);
        pathOnly.getConfig()
                .getConfData()
                .remove(PythonBridgeManager.PYTHON_ARTIFACT_SHA256_CONFIG);
        AgentPlan digestOnly = pythonPlan(artifact);
        digestOnly
                .getConfig()
                .getConfData()
                .remove(PythonBridgeManager.PYTHON_ARTIFACT_PATH_CONFIG);

        try (PythonBridgeManager bridge = new PythonBridgeManager()) {
            assertThatThrownBy(() -> bridge.validatePythonPlanMetadata(pathOnly))
                    .hasMessageContaining("must be configured together");
            assertThatThrownBy(() -> bridge.validatePythonPlanMetadata(digestOnly))
                    .hasMessageContaining("must be configured together");
        }
    }

    @Test
    void pythonArtifactMustBeARegularFile(@TempDir Path tempDir) throws Exception {
        try (PythonBridgeManager bridge = new PythonBridgeManager()) {
            assertThatThrownBy(() -> bridge.validatePythonPlanMetadata(pythonPlan(tempDir)))
                    .hasMessageContaining("regular file");
        }
    }

    @Test
    void overwritingOnePythonArtifactPathWithAnotherDigestIsRejected(@TempDir Path tempDir)
            throws Exception {
        Path artifact = pythonArtifact(tempDir.resolve("agent.zip"));
        AgentPlan current = pythonPlan(artifact);
        AgentPlan update = pythonPlan(artifact);
        current.getConfig()
                .setStr(PythonBridgeManager.PYTHON_ARTIFACT_SHA256_CONFIG, "1".repeat(64));
        update.getConfig()
                .setStr(PythonBridgeManager.PYTHON_ARTIFACT_SHA256_CONFIG, "2".repeat(64));

        try (PythonBridgeManager bridge = new PythonBridgeManager()) {
            assertThatThrownBy(() -> bridge.validatePythonPlanTransition(current, update))
                    .hasMessageContaining("python-artifact.path")
                    .hasMessageContaining("immutable");
        }
    }

    @Test
    void openIsNoOpWhenPlanHasNeitherPythonActionsNorResources() throws Exception {
        // Java-only plan: one Java action, no resources.
        Action javaAction = TestActions.noopAction();
        Map<String, Action> actions = Map.of(javaAction.getName(), javaAction);
        Map<String, List<Action>> byEvent = Map.of(InputEvent.class.getName(), List.of(javaAction));
        AgentPlan plan = new AgentPlan(actions, byEvent);

        try (PythonBridgeManager bridge = new PythonBridgeManager()) {
            bridge.open(
                    plan,
                    /* resourceCache */ null,
                    new ExecutionConfig(),
                    /* distributedCache */ null,
                    /* tmpDirs */ new String[] {System.getProperty("java.io.tmpdir")},
                    /* jobId */ new JobID(),
                    /* metricGroup */ null,
                    /* mailboxThreadChecker */ () -> {},
                    /* jobIdentifier */ "job-1",
                    /* userCodeClassLoader */ Thread.currentThread().getContextClassLoader());

            // No-op contract: nothing initialized, no Pemja interpreter created.
            assertThat(bridge.isInitialized()).isFalse();
            assertThat(bridge.getPythonActionExecutor()).isNull();
            assertThat(bridge.getPythonRunnerContext()).isNull();
        }
    }

    @Test
    void runtimeCloseStillClosesInterpreterWhenExecutorCloseFails() throws Exception {
        PythonActionExecutor executor = mock(PythonActionExecutor.class);
        PythonInterpreter interpreter = mock(PythonInterpreter.class);
        doThrow(new Exception("executor close failed")).when(executor).close();

        PythonBridgeManager.PlanPythonRuntime runtime =
                new PythonBridgeManager.PlanPythonRuntime(
                        executor, null, null, null, null, interpreter);

        assertThatThrownBy(runtime::close).hasMessageContaining("executor close failed");
        verify(interpreter).close();
    }

    @Test
    void nonZipPythonArtifactIsRejectedBeforeStartingCPython(@TempDir Path tempDir)
            throws Exception {
        Path artifact = tempDir.resolve("agent.zip");
        Files.writeString(artifact, "not a zip archive");

        try (PythonBridgeManager bridge = new PythonBridgeManager()) {
            assertThatThrownBy(() -> bridge.validatePythonPlanMetadata(pythonPlan(artifact)))
                    .hasMessageContaining("valid zip or wheel");
            assertThat(bridge.isInitialized()).isFalse();
        }
    }

    private static AgentPlan pythonPlan(Path artifact) throws Exception {
        Action action =
                new Action(
                        "pythonAction",
                        new PythonFunction("app.agent", "handle"),
                        List.of(InputEvent.EVENT_TYPE));
        Map<String, Action> actions = Map.of(action.getName(), action);
        Map<String, List<Action>> byEvent = Map.of(InputEvent.EVENT_TYPE, List.of(action));
        AgentConfiguration config = new AgentConfiguration();
        config.setStr(PythonBridgeManager.PYTHON_ARTIFACT_PATH_CONFIG, artifact.toString());
        if (Files.isRegularFile(artifact)) {
            config.setStr(
                    PythonBridgeManager.PYTHON_ARTIFACT_SHA256_CONFIG,
                    PlanIds.sha256HexOfFile(artifact));
        } else {
            config.setStr(PythonBridgeManager.PYTHON_ARTIFACT_SHA256_CONFIG, "0".repeat(64));
        }
        return new AgentPlan(actions, byEvent, new HashMap<>(), config);
    }

    private static Path pythonArtifact(Path artifact) throws Exception {
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(artifact))) {
            zip.putNextEntry(new ZipEntry("app/agent.py"));
            zip.write("def handle(value):\n    return value\n".getBytes());
            zip.closeEntry();
        }
        return artifact;
    }
}

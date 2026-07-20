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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.plan.AgentConfiguration;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.JavaFunction;
import org.apache.flink.agents.plan.PythonFunction;
import org.apache.flink.agents.plan.actions.Action;
import org.apache.flink.agents.runtime.actionstate.ActionState;
import org.apache.flink.agents.runtime.actionstate.InMemoryActionStateStore;
import org.apache.flink.agents.runtime.operator.coordinator.PlanIds;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateEvent;
import org.apache.flink.agents.runtime.python.utils.JavaResourceAdapter;
import org.apache.flink.agents.runtime.python.utils.PythonActionExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the checkpoint-aligned plan switch of {@link ActionExecutionOperator}: an update received
 * from the coordinator is held as pending, every record before the checkpoint barrier is processed
 * by the old plan, the switch happens at {@code prepareSnapshotPreBarrier} after draining in-flight
 * chains, and the snapshot records the switched plan.
 */
public class DynamicPlanSwitchOperatorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void updateIsHeldUntilCheckpointAndSwitchesAtBarrier() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            harness.processElement(new StreamRecord<>(1L));
            op.waitInFlightEventsFinished();
            assertThat(outputs(harness)).containsExactly("old:1");

            op.handleOperatorEvent(planUpdate(1L, json(singleActionPlan("newAction"))));
            assertThat(op.hasPendingPlanForTesting()).isTrue();
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(0L);

            // Records before the barrier still run on the old plan.
            harness.processElement(new StreamRecord<>(2L));
            op.waitInFlightEventsFinished();
            assertThat(outputs(harness)).containsExactly("old:1", "old:2");

            // The barrier: prepareSnapshotPreBarrier drains and switches, the snapshot
            // records the new plan.
            checkpoint(harness, 1L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);
            assertThat(op.hasPendingPlanForTesting()).isFalse();
            assertThat(op.getCurrentPlanForTesting().getActions()).containsOnlyKeys("newAction");

            harness.processElement(new StreamRecord<>(3L));
            op.waitInFlightEventsFinished();
            assertThat(outputs(harness)).containsExactly("old:1", "old:2", "new:3");
        }
    }

    @Test
    void barrierDrainsInFlightChainsOnTheOldPlanBeforeSwitching() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            // Admit two records (distinct keys) without running their action mails, then make an
            // update pending: the barrier must finish both chains on the old plan first.
            harness.processElement(new StreamRecord<>(7L));
            harness.processElement(new StreamRecord<>(8L));
            op.handleOperatorEvent(planUpdate(1L, json(singleActionPlan("newAction"))));

            checkpoint(harness, 1L);

            assertThat(outputs(harness)).containsExactlyInAnyOrder("old:7", "old:8");
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);

            harness.processElement(new StreamRecord<>(9L));
            op.waitInFlightEventsFinished();
            assertThat(outputs(harness)).contains("new:9");
        }
    }

    @Test
    void snapshotOfTheSwitchCheckpointRestoresTheNewPlan() throws Exception {
        String newPlanJson = json(singleActionPlan("newAction"));
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);
            op.handleOperatorEvent(planUpdate(1L, newPlanJson));
            // The switch happens at this snapshot's barrier, so this very checkpoint already
            // contains the new plan (with an empty in-flight set).
            snapshot = checkpoint(harness, 1L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);
        }
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> restored = harness()) {
            restored.initializeState(snapshot);
            restored.open();
            ActionExecutionOperator<Long, Object> op = op(restored);

            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);
            assertThat(op.getCurrentPlanForTesting().getActions()).containsOnlyKeys("newAction");

            restored.processElement(new StreamRecord<>(4L));
            op.waitInFlightEventsFinished();
            assertThat(outputs(restored)).containsExactly("new:4");
        }
    }

    @Test
    void samePlanIdReusesActionStateAcrossPlanVersions() throws Exception {
        AgentPlan bootstrapPlan = singleActionPlan("oldAction");
        AgentPlan repeatedPlan = singleActionPlan("newAction");
        String repeatedPlanJson = canonicalJson(repeatedPlan);
        String repeatedPlanId = PlanIds.planIdOf(repeatedPlanJson);
        InMemoryActionStateStore store = new InMemoryActionStateStore(false);
        InputEvent inputEvent = new InputEvent(42L);
        ActionState state = new ActionState(inputEvent);

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness =
                harness(bootstrapPlan, store)) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            op.handleOperatorEvent(new PlanUpdateEvent(1L, repeatedPlanId, repeatedPlanJson));
            checkpoint(harness, 1L);
            Action firstAction = op.getCurrentPlanForTesting().getActions().get("newAction");
            store.put("scope-key", 42L, firstAction, inputEvent, state);

            op.handleOperatorEvent(new PlanUpdateEvent(2L, repeatedPlanId, repeatedPlanJson));
            checkpoint(harness, 2L);
            Action secondAction = op.getCurrentPlanForTesting().getActions().get("newAction");

            assertThat(store.get("scope-key", 42L, secondAction, inputEvent)).isSameAs(state);
        }
    }

    @Test
    void reusedPlanVersionDoesNotReplayStateFromDifferentCandidate() throws Exception {
        AgentPlan bootstrapPlan = singleActionPlan("oldAction");
        InMemoryActionStateStore store = new InMemoryActionStateStore(false);
        InputEvent inputEvent = new InputEvent(42L);
        ActionState candidateAState = new ActionState(inputEvent);
        OperatorSubtaskState bootstrapSnapshot;

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness =
                harness(bootstrapPlan, store)) {
            harness.open();
            bootstrapSnapshot = checkpoint(harness, 1L);

            AgentPlan candidateA = configuredActionPlan("candidate-a");
            String candidateAJson = canonicalJson(candidateA);
            op(harness)
                    .handleOperatorEvent(
                            new PlanUpdateEvent(
                                    1L, PlanIds.planIdOf(candidateAJson), candidateAJson));
            checkpoint(harness, 2L);
            Action actionA =
                    op(harness).getCurrentPlanForTesting().getActions().get("configuredAction");
            store.put("scope-key", 42L, actionA, inputEvent, candidateAState);
        }

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> restored =
                harness(bootstrapPlan, store)) {
            restored.initializeState(bootstrapSnapshot);
            restored.open();

            AgentPlan candidateB = configuredActionPlan("candidate-b");
            String candidateBJson = canonicalJson(candidateB);
            op(restored)
                    .handleOperatorEvent(
                            new PlanUpdateEvent(
                                    1L, PlanIds.planIdOf(candidateBJson), candidateBJson));
            checkpoint(restored, 3L);
            Action actionB =
                    op(restored).getCurrentPlanForTesting().getActions().get("configuredAction");

            assertThat(store.get("scope-key", 42L, actionB, inputEvent)).isNull();
        }
    }

    @Test
    void coordinatorPlanIdSurvivesEffectivePlanSnapshotAndRestore() throws Exception {
        AgentPlan bootstrapPlan = singleActionPlan("oldAction");
        String bootstrapPlanId =
                PlanIds.planIdOf(PlanVersionManager.canonicalJsonOf(bootstrapPlan));
        AgentPlan submittedPlan = singleActionPlan("newAction");
        submittedPlan.getConfig().setStr("update-only-key", "discarded-by-config-pinning");
        String submittedPlanJson = canonicalJson(submittedPlan);
        String coordinatorPlanId = PlanIds.planIdOf(submittedPlanJson);
        OperatorSubtaskState snapshot;

        InMemoryActionStateStore firstStore = new InMemoryActionStateStore(false);
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness =
                harness(bootstrapPlan, firstStore)) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);
            assertThat(op.getCurrentPlanIdForTesting()).isEqualTo(bootstrapPlanId);
            assertThat(firstStore.getActivePlanId()).isEqualTo(bootstrapPlanId);

            op.handleOperatorEvent(new PlanUpdateEvent(1L, coordinatorPlanId, submittedPlanJson));
            snapshot = checkpoint(harness, 1L);

            assertThat(op.getCurrentPlanIdForTesting()).isEqualTo(coordinatorPlanId);
            assertThat(firstStore.getActivePlanId()).isEqualTo(coordinatorPlanId);
            assertThat(
                            PlanIds.planIdOf(
                                    PlanVersionManager.canonicalJsonOf(
                                            op.getCurrentPlanForTesting())))
                    .isNotEqualTo(coordinatorPlanId);
        }

        InMemoryActionStateStore restoredStore = new InMemoryActionStateStore(false);
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> restored =
                harness(bootstrapPlan, restoredStore)) {
            restored.initializeState(snapshot);
            restored.open();

            assertThat(op(restored).getCurrentPlanIdForTesting()).isEqualTo(coordinatorPlanId);
            assertThat(restoredStore.getActivePlanId()).isEqualTo(coordinatorPlanId);
        }
    }

    @Test
    void restoreWithoutSwitchKeepsBootstrapPlanUntilUpdateIsResubmitted() throws Exception {
        String newPlanJson = json(singleActionPlan("newAction"));
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            // Snapshot before any update: the bootstrap plan is in effect and nothing is written.
            snapshot = checkpoint(harness, 1L);
        }
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> restored = harness()) {
            restored.initializeState(snapshot);
            restored.open();
            ActionExecutionOperator<Long, Object> op = op(restored);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(0L);

            // The volatile candidate was discarded by recovery. A client resubmission is a fresh
            // pending update and switches at the next checkpoint.
            op.handleOperatorEvent(planUpdate(1L, newPlanJson));
            assertThat(op.hasPendingPlanForTesting()).isTrue();
            checkpoint(restored, 2L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);
        }
    }

    @Test
    void staleOrDuplicateDeliveryIsIgnored() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            String newPlanJson = json(singleActionPlan("newAction"));
            op.handleOperatorEvent(planUpdate(1L, newPlanJson));
            checkpoint(harness, 1L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);

            // Re-delivery of the switched plan must not create pending work; neither must an older
            // version.
            op.handleOperatorEvent(planUpdate(1L, newPlanJson));
            assertThat(op.hasPendingPlanForTesting()).isFalse();
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);
        }
    }

    @Test
    void newestPendingWinsBeforeTheSwitch() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            op.handleOperatorEvent(planUpdate(1L, json(singleActionPlan("newAction"))));
            op.handleOperatorEvent(planUpdate(2L, json(singleActionPlan("newerAction"))));

            checkpoint(harness, 1L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(2L);
            assertThat(op.getCurrentPlanForTesting().getActions()).containsOnlyKeys("newerAction");
        }
    }

    @Test
    void localPrepareFailureIsRethrownAfterPendingIsDiscarded() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            assertThatThrownBy(() -> op.handleOperatorEvent(planUpdate(1L, "{not-a-plan")))
                    .hasRootCauseInstanceOf(JsonParseException.class);
            assertThat(op.hasPendingPlanForTesting()).isFalse();
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(0L);
        }
    }

    @Test
    void updateConfigIsPinnedToBootstrapPlan(@TempDir Path tempDir) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            AgentPlan newPlan = singleActionPlan("newAction");
            newPlan.getConfig().setStr("job-identifier", "attempted-override");
            newPlan.getConfig().setInt("num-async-threads", 1);
            newPlan.getConfig().setStr("update-only-key", "ignored");
            Path artifact = writeJavaArtifact(tempDir.resolve("plan-v1.jar"), "v1");
            configureJavaArtifact(newPlan, artifact);
            op.handleOperatorEvent(planUpdate(1L, json(newPlan)));
            checkpoint(harness, 1L);

            // Job-level configuration is pinned at submission; artifact coordinates are the
            // plan-scoped exception.
            AgentConfiguration effective = op.getCurrentPlanForTesting().getConfig();
            assertThat(effective.getStr("job-identifier", null)).isNull();
            assertThat(effective.getInt("num-async-threads", -1)).isEqualTo(-1);
            assertThat(effective.getStr("update-only-key", null)).isNull();
            assertThat(effective.getStr(PlanVersionManager.JAVA_ARTIFACT_PATH_CONFIG, null))
                    .isEqualTo(artifact.toString());
            assertThat(effective.getStr(PlanVersionManager.JAVA_ARTIFACT_SHA256_CONFIG, null))
                    .isEqualTo(PlanIds.sha256HexOfFile(artifact));
        }
    }

    @Test
    void javaArtifactPathAndShaMustBeConfiguredTogether(@TempDir Path tempDir) throws Exception {
        Path artifact = writeJavaArtifact(tempDir.resolve("user-actions.jar"), "v1");

        AgentPlan pathOnly = singleActionPlan("newAction");
        pathOnly.getConfig()
                .setStr(PlanVersionManager.JAVA_ARTIFACT_PATH_CONFIG, artifact.toString());
        AgentPlan shaOnly = singleActionPlan("newAction");
        shaOnly.getConfig()
                .setStr(
                        PlanVersionManager.JAVA_ARTIFACT_SHA256_CONFIG,
                        PlanIds.sha256HexOfFile(artifact));

        for (AgentPlan invalidPlan : List.of(pathOnly, shaOnly)) {
            try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
                harness.open();
                ActionExecutionOperator<Long, Object> op = op(harness);

                assertThatThrownBy(() -> op.handleOperatorEvent(planUpdate(1L, json(invalidPlan))))
                        .hasMessageContaining("must be configured together");

                assertThat(op.hasPendingPlanForTesting()).isFalse();
                assertThat(op.getCurrentPlanVersionForTesting()).isZero();
            }
        }
    }

    @Test
    void javaArtifactTamperedAfterPrepareFailsAtTheActivationBarrier(@TempDir Path tempDir)
            throws Exception {
        Path artifact = writeJavaArtifact(tempDir.resolve("user-actions.jar"), "v1");
        AgentPlan newPlan = javaArtifactPlan(artifact);

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            op.handleOperatorEvent(planUpdate(1L, json(newPlan)));
            assertThat(op.hasPendingPlanForTesting()).isTrue();
            writeJavaArtifact(artifact, "tampered-after-prepare");

            assertThatThrownBy(() -> checkpoint(harness, 1L))
                    .hasMessageContaining("failed sha256 verification")
                    .hasMessageContaining(artifact.toString());
        }
    }

    @Test
    void javaArtifactCannotChangeDigestAtTheSamePath(@TempDir Path tempDir) throws Exception {
        Path artifact = writeJavaArtifact(tempDir.resolve("user-actions.jar"), "v1");
        AgentPlan versionOne = javaArtifactPlan(artifact);
        String versionOneDigest =
                versionOne.getConfig().getStr(PlanVersionManager.JAVA_ARTIFACT_SHA256_CONFIG, null);

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);
            op.handleOperatorEvent(planUpdate(1L, json(versionOne)));
            checkpoint(harness, 1L);

            writeJavaArtifact(artifact, "v2");
            AgentPlan versionTwo = javaArtifactPlan(artifact);
            assertThat(
                            versionTwo
                                    .getConfig()
                                    .getStr(PlanVersionManager.JAVA_ARTIFACT_SHA256_CONFIG, null))
                    .isNotEqualTo(versionOneDigest);

            assertThatThrownBy(() -> op.handleOperatorEvent(planUpdate(2L, json(versionTwo))))
                    .hasMessageContaining("is immutable");

            assertThat(op.hasPendingPlanForTesting()).isFalse();
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);
        }
    }

    @Test
    void javaArtifactLoadsActionClassAbsentFromTheJobClasspath(@TempDir Path tempDir)
            throws Exception {
        String className = "user.dynamic.ExternalAction";
        Path artifact = createExternalJavaActionJar(tempDir.resolve("v1"), className, 100L);

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            op.handleOperatorEvent(
                    planUpdate(1L, json(externalJavaArtifactPlan(artifact, className))));
            checkpoint(harness, 1L);
            harness.processElement(new StreamRecord<>(1L));
            op.waitInFlightEventsFinished();

            assertThat(outputs(harness)).containsExactly(101L);
        }
    }

    @Test
    void restoredJavaArtifactPlanRecreatesItsClassLoader(@TempDir Path tempDir) throws Exception {
        String className = "user.dynamic.ExternalAction";
        Path artifact = createExternalJavaActionJar(tempDir.resolve("v2"), className, 200L);
        OperatorSubtaskState snapshot;

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);
            op.handleOperatorEvent(
                    planUpdate(1L, json(externalJavaArtifactPlan(artifact, className))));
            snapshot = checkpoint(harness, 1L);
        }

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> restored = harness()) {
            restored.initializeState(snapshot);
            restored.open();
            ActionExecutionOperator<Long, Object> op = op(restored);
            restored.processElement(new StreamRecord<>(1L));
            op.waitInFlightEventsFinished();

            assertThat(outputs(restored)).containsExactly(201L);
        }
    }

    @Test
    void pythonBridgeUsesTheActivePlanJavaArtifactClassLoaderAfterSwitchAndRestore(
            @TempDir Path tempDir) throws Exception {
        String className = "user.dynamic.ExternalAction";
        Path artifact = createExternalJavaActionJar(tempDir.resolve("bridge"), className, 300L);
        OperatorSubtaskState snapshot;

        try (KeyedOneInputStreamOperatorTestHarness<Long, Row, Object> harness =
                pythonWireHarness()) {
            harness.open();
            ActionExecutionOperator<Row, Object> op = rowOp(harness);
            op.handleOperatorEvent(
                    planUpdate(1L, json(externalJavaArtifactPlan(artifact, className))));
            harness.prepareSnapshotPreBarrier(1L);
            snapshot = harness.snapshot(1L, 1L);

            Object result =
                    javaResourceAdapter(pythonBridge(op))
                            .invokeJavaAction(className, "probe", List.of(), List.of());

            assertThat(result).isEqualTo(300L);
        }

        try (KeyedOneInputStreamOperatorTestHarness<Long, Row, Object> restored =
                pythonWireHarness()) {
            restored.initializeState(snapshot);
            restored.open();

            Object result =
                    javaResourceAdapter(pythonBridge(rowOp(restored)))
                            .invokeJavaAction(className, "probe", List.of(), List.of());

            assertThat(result).isEqualTo(300L);
        }
    }

    @Test
    void pythonArtifactTamperedAfterPrepareFailsAtTheActivationBarrier(@TempDir Path tempDir)
            throws Exception {
        Path artifact = writePythonArtifact(tempDir.resolve("agent-v2.zip"));

        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness =
                harness(pythonPlan("old_action"))) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            op.handleOperatorEvent(planUpdate(1L, json(pythonArtifactPlan(artifact))));
            assertThat(op.hasPendingPlanForTesting()).isTrue();
            Files.writeString(artifact, "tampered after prepare");

            assertThatThrownBy(() -> checkpoint(harness, 1L))
                    .hasMessageContaining("failed sha256 verification")
                    .hasMessageContaining(artifact.toString());
        }
    }

    @Test
    void existingPythonActionCanSwitchWithoutAnArtifact() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness =
                harness(pythonPlan("old_action"))) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);
            PythonBridgeManager bridge = pythonBridge(op);
            PythonActionExecutor oldExecutor = bridge.getPythonActionExecutor();

            harness.processElement(new StreamRecord<>(1L));
            op.waitInFlightEventsFinished();
            assertThat(outputs(harness)).containsExactly("python-old:1");

            op.handleOperatorEvent(planUpdate(1L, json(pythonPlan("new_action"))));
            assertThat(bridge.getPythonActionExecutor()).isSameAs(oldExecutor);

            checkpoint(harness, 1L);
            assertThat(bridge.getPythonActionExecutor()).isNotSameAs(oldExecutor);

            harness.processElement(new StreamRecord<>(2L));
            op.waitInFlightEventsFinished();
            assertThat(outputs(harness)).containsExactly("python-old:1", "python-new:2");
        }
    }

    @Test
    void javaOnlyPlansKeepThePythonWireBridgeAcrossSwitch() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Row, Object> harness =
                pythonWireHarness()) {
            harness.open();
            ActionExecutionOperator<Row, Object> op = rowOp(harness);
            PythonBridgeManager bridge = pythonBridge(op);
            PythonActionExecutor oldExecutor = bridge.getPythonActionExecutor();

            assertThat(oldExecutor).isNotNull();

            op.handleOperatorEvent(planUpdate(1L, json(singleActionPlan("newAction"))));
            harness.prepareSnapshotPreBarrier(1L);
            harness.snapshot(1L, 1L);

            assertThat(bridge.getPythonActionExecutor()).isNotSameAs(oldExecutor);
        }
    }

    @Test
    void javaRunnerContextIsReboundToTheActivatedPlan() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness =
                harness(configuredActionPlan("old-context"))) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            harness.processElement(new StreamRecord<>(1L));
            op.waitInFlightEventsFinished();

            op.handleOperatorEvent(planUpdate(1L, json(configuredActionPlan("new-context"))));
            checkpoint(harness, 1L);
            harness.processElement(new StreamRecord<>(2L));
            op.waitInFlightEventsFinished();

            assertThat(outputs(harness)).containsExactly("old-context", "new-context");
        }
    }

    // ------------------------------------------------------------------------------------------

    /** Mirrors the task's barrier sequence: prepareSnapshotPreBarrier, then the snapshot. */
    private static OperatorSubtaskState checkpoint(
            KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness, long checkpointId)
            throws Exception {
        harness.prepareSnapshotPreBarrier(checkpointId);
        return harness.snapshot(checkpointId, checkpointId);
    }

    private static PlanUpdateEvent planUpdate(long planVersion, String agentPlanJson) {
        return new PlanUpdateEvent(planVersion, PlanIds.planIdOf(agentPlanJson), agentPlanJson);
    }

    @SuppressWarnings("unchecked")
    private static ActionExecutionOperator<Long, Object> op(
            KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness) {
        return (ActionExecutionOperator<Long, Object>) harness.getOperator();
    }

    private static List<Object> outputs(
            KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness) {
        return harness.getRecordOutput().stream()
                .map(StreamRecord::getValue)
                .collect(Collectors.toList());
    }

    private static KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness()
            throws Exception {
        return harness(singleActionPlan("oldAction"));
    }

    private static KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness(
            AgentPlan plan) throws Exception {
        return harness(plan, null);
    }

    private static KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness(
            AgentPlan plan, InMemoryActionStateStore actionStateStore) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new ActionExecutionOperatorFactory<>(plan, true, actionStateStore),
                (KeySelector<Long, Long>) value -> value,
                TypeInformation.of(Long.class));
    }

    private static KeyedOneInputStreamOperatorTestHarness<Long, Row, Object> pythonWireHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new ActionExecutionOperatorFactory<>(singleActionPlan("oldAction"), false),
                (KeySelector<Row, Long>) value -> 0L,
                TypeInformation.of(Long.class));
    }

    @SuppressWarnings("unchecked")
    private static ActionExecutionOperator<Row, Object> rowOp(
            KeyedOneInputStreamOperatorTestHarness<Long, Row, Object> harness) {
        return (ActionExecutionOperator<Row, Object>) harness.getOperator();
    }

    private static PythonBridgeManager pythonBridge(ActionExecutionOperator<?, ?> operator)
            throws Exception {
        Field field = ActionExecutionOperator.class.getDeclaredField("pythonBridge");
        field.setAccessible(true);
        return (PythonBridgeManager) field.get(operator);
    }

    private static JavaResourceAdapter javaResourceAdapter(PythonBridgeManager bridge)
            throws Exception {
        Field activeRuntimeField = PythonBridgeManager.class.getDeclaredField("activeRuntime");
        activeRuntimeField.setAccessible(true);
        Object activeRuntime = activeRuntimeField.get(bridge);
        Field adapterField = activeRuntime.getClass().getDeclaredField("javaResourceAdapter");
        adapterField.setAccessible(true);
        return (JavaResourceAdapter) adapterField.get(activeRuntime);
    }

    private static String json(AgentPlan plan) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(plan);
    }

    private static String canonicalJson(AgentPlan plan) throws Exception {
        return PlanIds.canonicalize(json(plan));
    }

    public static void oldAction(Event event, RunnerContext context) {
        context.sendEvent(new OutputEvent("old:" + InputEvent.fromEvent(event).getInput()));
    }

    public static void newAction(Event event, RunnerContext context) {
        context.sendEvent(new OutputEvent("new:" + InputEvent.fromEvent(event).getInput()));
    }

    public static void newerAction(Event event, RunnerContext context) {
        context.sendEvent(new OutputEvent("newer:" + InputEvent.fromEvent(event).getInput()));
    }

    public static void configuredAction(Event event, RunnerContext context) {
        context.sendEvent(new OutputEvent(context.getActionConfigValue("marker")));
    }

    private static AgentPlan singleActionPlan(String method) throws Exception {
        Action action =
                new Action(
                        method,
                        new JavaFunction(
                                DynamicPlanSwitchOperatorTest.class,
                                method,
                                new Class<?>[] {Event.class, RunnerContext.class}),
                        Collections.singletonList(InputEvent.EVENT_TYPE));
        Map<String, Action> actions = new HashMap<>();
        actions.put(action.getName(), action);
        Map<String, List<Action>> byEvent = new HashMap<>();
        byEvent.put(InputEvent.EVENT_TYPE, Collections.singletonList(action));
        AgentConfiguration config = new AgentConfiguration();
        return new AgentPlan(actions, byEvent, new HashMap<>(), config);
    }

    private static AgentPlan configuredActionPlan(String marker) throws Exception {
        Action action =
                new Action(
                        "configuredAction",
                        new JavaFunction(
                                DynamicPlanSwitchOperatorTest.class,
                                "configuredAction",
                                new Class<?>[] {Event.class, RunnerContext.class}),
                        Collections.singletonList(InputEvent.EVENT_TYPE),
                        new HashMap<>(Map.of("marker", marker)));
        return new AgentPlan(
                Map.of(action.getName(), action),
                Map.of(InputEvent.EVENT_TYPE, List.of(action)),
                new HashMap<>(),
                new AgentConfiguration());
    }

    private static AgentPlan pythonPlan(String functionName) throws Exception {
        Action action =
                new Action(
                        "pythonAction",
                        new PythonFunction(
                                "flink_agents.plan.tests.dynamic_plan_test_actions", functionName),
                        Collections.singletonList(InputEvent.EVENT_TYPE));
        AgentConfiguration config = new AgentConfiguration();
        config.setInt("num-async-threads", 1);
        return new AgentPlan(
                Map.of(action.getName(), action),
                Map.of(InputEvent.EVENT_TYPE, List.of(action)),
                new HashMap<>(),
                config);
    }

    private static AgentPlan pythonArtifactPlan(Path artifact) throws Exception {
        AgentPlan plan = pythonPlan("new_action");
        plan.getConfig()
                .setStr(PythonBridgeManager.PYTHON_ARTIFACT_PATH_CONFIG, artifact.toString());
        plan.getConfig()
                .setStr(
                        PythonBridgeManager.PYTHON_ARTIFACT_SHA256_CONFIG,
                        PlanIds.sha256HexOfFile(artifact));
        return plan;
    }

    private static AgentPlan javaArtifactPlan(Path artifactPath) throws Exception {
        AgentPlan plan = singleActionPlan("newAction");
        configureJavaArtifact(plan, artifactPath);
        return plan;
    }

    private static AgentPlan externalJavaArtifactPlan(Path artifactPath, String className)
            throws Exception {
        Action action =
                new Action(
                        "externalJavaAction",
                        new JavaFunction(
                                className,
                                "execute",
                                new Class<?>[] {Event.class, RunnerContext.class}),
                        Collections.singletonList(InputEvent.EVENT_TYPE));
        AgentPlan plan =
                new AgentPlan(
                        Map.of(action.getName(), action),
                        Map.of(InputEvent.EVENT_TYPE, List.of(action)),
                        new HashMap<>(),
                        new AgentConfiguration());
        configureJavaArtifact(plan, artifactPath);
        return plan;
    }

    private static void configureJavaArtifact(AgentPlan plan, Path artifactPath) throws Exception {
        plan.getConfig()
                .setStr(PlanVersionManager.JAVA_ARTIFACT_PATH_CONFIG, artifactPath.toString());
        plan.getConfig()
                .setStr(
                        PlanVersionManager.JAVA_ARTIFACT_SHA256_CONFIG,
                        PlanIds.sha256HexOfFile(artifactPath));
    }

    private static Path createExternalJavaActionJar(Path directory, String className, long addend)
            throws Exception {
        String packageName = className.substring(0, className.lastIndexOf('.'));
        String simpleName = className.substring(className.lastIndexOf('.') + 1);
        String source =
                "package "
                        + packageName
                        + ";\n"
                        + "import org.apache.flink.agents.api.Event;\n"
                        + "import org.apache.flink.agents.api.InputEvent;\n"
                        + "import org.apache.flink.agents.api.OutputEvent;\n"
                        + "import org.apache.flink.agents.api.context.RunnerContext;\n"
                        + "public class "
                        + simpleName
                        + " {\n"
                        + "  public static void execute(Event event, RunnerContext context) {\n"
                        + "    Long input = (Long) InputEvent.fromEvent(event).getInput();\n"
                        + "    context.sendEvent(new OutputEvent(input + "
                        + addend
                        + "L));\n"
                        + "  }\n"
                        + "  public static long probe() { return "
                        + addend
                        + "L; }\n"
                        + "}\n";

        Path sourceRoot = directory.resolve("src");
        Path classesRoot = directory.resolve("classes");
        Path sourceFile =
                sourceRoot.resolve(packageName.replace('.', '/')).resolve(simpleName + ".java");
        Files.createDirectories(sourceFile.getParent());
        Files.createDirectories(classesRoot);
        Files.writeString(sourceFile, source, StandardCharsets.UTF_8);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).as("JDK compiler must be available").isNotNull();
        try (StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
            Boolean success =
                    compiler.getTask(
                                    null,
                                    fileManager,
                                    null,
                                    List.of(
                                            "-classpath",
                                            testCompilerClasspath(),
                                            "-d",
                                            classesRoot.toString()),
                                    null,
                                    fileManager.getJavaFileObjects(sourceFile))
                            .call();
            assertThat(success).isTrue();
        }

        Path artifact = directory.resolve("external-action-" + addend + ".jar");
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(artifact))) {
            Path classFile =
                    classesRoot
                            .resolve(packageName.replace('.', '/'))
                            .resolve(simpleName + ".class");
            jar.putNextEntry(
                    new JarEntry(packageName.replace('.', '/') + "/" + simpleName + ".class"));
            Files.copy(classFile, jar);
            jar.closeEntry();
        }
        return artifact;
    }

    private static Path writeJavaArtifact(Path artifact, String marker) throws Exception {
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(artifact))) {
            jar.putNextEntry(new JarEntry("marker.txt"));
            jar.write(marker.getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
        return artifact;
    }

    private static Path writePythonArtifact(Path artifact) throws Exception {
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(artifact))) {
            zip.putNextEntry(new ZipEntry("app/agent.py"));
            zip.write("def handle(value):\n    return value\n".getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return artifact;
    }

    private static String testCompilerClasspath() throws Exception {
        LinkedHashSet<String> entries = new LinkedHashSet<>();
        addCodeSource(entries, Event.class);
        addCodeSource(entries, InputEvent.class);
        addCodeSource(entries, OutputEvent.class);
        addCodeSource(entries, RunnerContext.class);
        for (String entry :
                System.getProperty("java.class.path").split(java.io.File.pathSeparator)) {
            if (!entry.isEmpty()) {
                entries.add(entry);
            }
        }
        return String.join(java.io.File.pathSeparator, entries);
    }

    private static void addCodeSource(LinkedHashSet<String> entries, Class<?> clazz)
            throws Exception {
        entries.add(clazz.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
    }
}

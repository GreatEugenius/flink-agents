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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.plan.AgentConfiguration;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.JavaFunction;
import org.apache.flink.agents.plan.actions.Action;
import org.apache.flink.agents.runtime.operator.coordinator.PlanIds;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

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
    void restoreWithoutSwitchKeepsBootstrapPlanAndBackfillReappliesUpdate() throws Exception {
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

            // The coordinator backfills its latest plan after every restore; the subtask treats it
            // as a fresh pending update and switches at the next checkpoint.
            op.handleOperatorEvent(planUpdate(1L, newPlanJson));
            assertThat(op.hasPendingPlanForTesting()).isTrue();
            checkpoint(restored, 2L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);
        }
    }

    @Test
    void staleOrDuplicateBackfillIsIgnored() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            String newPlanJson = json(singleActionPlan("newAction"));
            op.handleOperatorEvent(planUpdate(1L, newPlanJson));
            checkpoint(harness, 1L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(1L);

            // Re-delivery of the switched plan (coordinator backfill) must not create pending
            // work; neither must an older sequence.
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
    void rejectedUpdateKeepsTheCurrentPlan() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            op.handleOperatorEvent(planUpdate(1L, "{not-a-plan"));
            assertThat(op.hasPendingPlanForTesting()).isFalse();
            checkpoint(harness, 1L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(0L);

            // A later valid update is unaffected by the earlier rejection.
            op.handleOperatorEvent(planUpdate(2L, json(singleActionPlan("newAction"))));
            checkpoint(harness, 2L);
            assertThat(op.getCurrentPlanVersionForTesting()).isEqualTo(2L);
        }
    }

    @Test
    void updateConfigIsIgnoredExceptDynamicPlanKeys() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness = harness()) {
            harness.open();
            ActionExecutionOperator<Long, Object> op = op(harness);

            AgentPlan newPlan = singleActionPlan("newAction");
            newPlan.getConfig().setStr("job-identifier", "attempted-override");
            newPlan.getConfig().setInt("num-async-threads", 1);
            newPlan.getConfig().setStr("dynamic-plan.python-runtime.path", "/plan/scoped");
            op.handleOperatorEvent(planUpdate(1L, json(newPlan)));
            checkpoint(harness, 1L);

            // Job-level configuration is pinned at submission: the effective plan carries the
            // bootstrap config, not the update's — except the update's own dynamic-plan.* keys,
            // which are plan-scoped by nature.
            AgentConfiguration effective = op.getCurrentPlanForTesting().getConfig();
            assertThat(effective.getStr("job-identifier", null)).isNull();
            assertThat(effective.getInt("num-async-threads", -1)).isEqualTo(-1);
            assertThat(effective.getStr("dynamic-plan.python-runtime.path", null))
                    .isEqualTo("/plan/scoped");
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
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new ActionExecutionOperatorFactory<>(singleActionPlan("oldAction"), true),
                (KeySelector<Long, Long>) value -> value,
                TypeInformation.of(Long.class));
    }

    private static String json(AgentPlan plan) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(plan);
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
}

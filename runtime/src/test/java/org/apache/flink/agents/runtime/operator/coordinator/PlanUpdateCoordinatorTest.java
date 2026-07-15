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
package org.apache.flink.agents.runtime.operator.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.plan.AgentConfiguration;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.JavaFunction;
import org.apache.flink.agents.plan.actions.Action;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateCoordinator.CoordinatorState;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateEvent;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateRequest;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateResponse;
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the two-phase switch state machine of {@link PlanUpdateCoordinator}. */
public class PlanUpdateCoordinatorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void updateIsStagedAndAnnouncedOnlyAfterTheStagingCheckpointCompletes() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        FakeGateway gateway = ready(coordinator, 0);

        PlanUpdateResponse response = submit(coordinator, json(plan("actionA")));
        assertThat(response.planVersion).isEqualTo(1L);
        // Phase 1: staged only — nothing goes out before the decision is durable.
        assertThat(gateway.sentEvents).isEmpty();

        CompletableFuture<byte[]> snapshot = new CompletableFuture<>();
        coordinator.checkpointCoordinator(10L, snapshot);
        CoordinatorState persisted = state(snapshot);
        assertThat(persisted.active).isNull();
        assertThat(persisted.staged.planVersion).isEqualTo(1L);
        assertThat(gateway.sentEvents).isEmpty();

        // The staging checkpoint completed: announce to every subtask.
        coordinator.notifyCheckpointComplete(10L);
        assertThat(gateway.sentEvents).hasSize(1);
        assertThat(((PlanUpdateEvent) gateway.sentEvents.get(0)).planVersion).isEqualTo(1L);
    }

    @Test
    void activationIsRecordedByTheCheckpointAfterTheAnnounce() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        ready(coordinator, 0);
        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L);

        // The checkpoint after the announce records active = staged: restoring it implies every
        // subtask switched at its barrier.
        CompletableFuture<byte[]> snapshot = new CompletableFuture<>();
        coordinator.checkpointCoordinator(11L, snapshot);
        CoordinatorState persisted = state(snapshot);
        assertThat(persisted.active.planVersion).isEqualTo(1L);
        assertThat(persisted.staged).isNull();
        coordinator.notifyCheckpointComplete(11L);

        // The next update is sequenced on top of the activated version.
        assertThat(submit(coordinator, json(plan("actionB"))).planVersion).isEqualTo(2L);
    }

    @Test
    void updatesAreRejectedWhileStagedPendingOrCheckpointInFlight() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        ready(coordinator, 0);
        String planJson = json(plan("actionA"));
        submit(coordinator, planJson);

        // One in-flight update at a time.
        assertThatThrownBy(() -> submit(coordinator, planJson))
                .rootCause()
                .hasMessageContaining("staged and not yet effective");

        // And while a checkpoint is in flight the gate stays shut as well.
        completeCheckpoint(coordinator, 10L);
        completeCheckpoint(coordinator, 11L); // activates v1
        coordinator.checkpointCoordinator(12L, new CompletableFuture<>());
        assertThatThrownBy(() -> submit(coordinator, planJson))
                .rootCause()
                .hasMessageContaining("checkpoint is in progress");
        coordinator.notifyCheckpointComplete(12L);
        assertThat(submit(coordinator, planJson).planVersion).isEqualTo(2L);
    }

    @Test
    void nextCheckpointSnapshotWaitsForAnnounceDeliveryConfirmations() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        FakeGateway gateway = ready(coordinator, 0);
        gateway.autoAck = false;

        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L); // announce sent, delivery unconfirmed

        // The barrier of the switch checkpoint must not exist before every subtask has the
        // instruction: the coordinator snapshot (which gates barrier injection) stays incomplete
        // until the delivery is confirmed.
        CompletableFuture<byte[]> snapshot = new CompletableFuture<>();
        coordinator.checkpointCoordinator(11L, snapshot);
        assertThat(snapshot).isNotDone();

        gateway.ackAll();
        assertThat(snapshot).isDone();
        assertThat(state(snapshot).active.planVersion).isEqualTo(1L);
    }

    @Test
    void abortedStagingCheckpointDefersTheAnnounce() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        FakeGateway gateway = ready(coordinator, 0);

        submit(coordinator, json(plan("actionA")));
        coordinator.checkpointCoordinator(10L, new CompletableFuture<>());
        coordinator.notifyCheckpointAborted(10L);
        // The decision never became durable: announcing would let subtasks switch to a plan that
        // a restore could not reconstruct.
        assertThat(gateway.sentEvents).isEmpty();

        completeCheckpoint(coordinator, 11L);
        assertThat(gateway.sentEvents).hasSize(1);
    }

    @Test
    void restoreReannouncesADurableStagedPlan() throws Exception {
        // Restore from a checkpoint that carries {active=v1, staged=v2}: the interrupted switch
        // must resume — both plans are re-delivered on executionAttemptReady.
        CoordinatorState restored =
                new CoordinatorState(
                        record(1L, json(plan("actionA"))), record(2L, json(plan("actionB"))));
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        coordinator.resetToCheckpoint(5L, OBJECT_MAPPER.writeValueAsBytes(restored));

        FakeGateway gateway = ready(coordinator, 0);
        // Only the staged plan is re-delivered: subtasks recover the active plan from their own
        // union state, so re-sending it would be a guaranteed no-op.
        assertThat(gateway.sentEvents).hasSize(1);
        assertThat(((PlanUpdateEvent) gateway.sentEvents.get(0)).planVersion).isEqualTo(2L);

        // The interrupted update still blocks new submissions until it activates.
        assertThatThrownBy(() -> submit(coordinator, json(plan("actionA"))))
                .rootCause()
                .hasMessageContaining("staged and not yet effective");
        completeCheckpoint(coordinator, 6L); // activates v2
        assertThat(submit(coordinator, json(plan("actionA"))).planVersion).isEqualTo(3L);
    }

    @Test
    void restoreWithEmptyStateClearsTheGates() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        coordinator.checkpointCoordinator(1L, new CompletableFuture<>());
        coordinator.resetToCheckpoint(1L, new byte[0]);
        ready(coordinator, 0);
        assertThat(submit(coordinator, json(plan("actionA"))).planVersion).isEqualTo(1L);
    }

    // ------------------------------------------------------------------------------------------

    private static PlanUpdateResponse submit(PlanUpdateCoordinator coordinator, String planJson)
            throws Exception {
        return (PlanUpdateResponse)
                coordinator.handleCoordinationRequest(new PlanUpdateRequest(planJson)).get();
    }

    private static void completeCheckpoint(PlanUpdateCoordinator coordinator, long checkpointId) {
        coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());
        coordinator.notifyCheckpointComplete(checkpointId);
    }

    private static CoordinatorState state(CompletableFuture<byte[]> snapshot) throws Exception {
        return OBJECT_MAPPER.readValue(snapshot.get(), CoordinatorState.class);
    }

    private static FakeGateway ready(PlanUpdateCoordinator coordinator, int subtask) {
        FakeGateway gateway = new FakeGateway(subtask);
        coordinator.executionAttemptReady(subtask, 0, gateway);
        return gateway;
    }

    private static PlanUpdateCoordinator.PlanRecord record(long version, String planJson)
            throws Exception {
        String canonical = PlanIds.canonicalize(planJson);
        return new PlanUpdateCoordinator.PlanRecord(
                version, PlanIds.planIdOf(canonical), canonical);
    }

    public static void actionA(Event event, RunnerContext context) {}

    public static void actionB(Event event, RunnerContext context) {}

    private static AgentPlan plan(String method) throws Exception {
        Action action =
                new Action(
                        method,
                        new JavaFunction(
                                PlanUpdateCoordinatorTest.class,
                                method,
                                new Class<?>[] {Event.class, RunnerContext.class}),
                        Collections.singletonList(InputEvent.EVENT_TYPE));
        Map<String, Action> actions = new HashMap<>();
        actions.put(action.getName(), action);
        Map<String, List<Action>> byEvent = new HashMap<>();
        byEvent.put(InputEvent.EVENT_TYPE, Collections.singletonList(action));
        return new AgentPlan(actions, byEvent, new HashMap<>(), new AgentConfiguration());
    }

    private static String json(AgentPlan plan) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(plan);
    }

    /** Records sent events; delivery confirmation is immediate unless {@code autoAck} is off. */
    private static final class FakeGateway implements OperatorCoordinator.SubtaskGateway {
        final List<OperatorEvent> sentEvents = new ArrayList<>();
        final List<CompletableFuture<Acknowledge>> unacked = new ArrayList<>();
        final int subtask;
        boolean autoAck = true;

        FakeGateway(int subtask) {
            this.subtask = subtask;
        }

        @Override
        public CompletableFuture<Acknowledge> sendEvent(OperatorEvent event) {
            sentEvents.add(event);
            CompletableFuture<Acknowledge> ack = new CompletableFuture<>();
            if (autoAck) {
                ack.complete(Acknowledge.get());
            } else {
                unacked.add(ack);
            }
            return ack;
        }

        void ackAll() {
            unacked.forEach(f -> f.complete(Acknowledge.get()));
            unacked.clear();
        }

        @Override
        public ExecutionAttemptID getExecution() {
            return ExecutionAttemptID.randomId();
        }

        @Override
        public int getSubtask() {
            return subtask;
        }
    }

    /** Minimal context: the coordinator only reads {@link #currentParallelism()}. */
    private static final class FakeContext implements OperatorCoordinator.Context {
        @Override
        public JobID getJobID() {
            return new JobID();
        }

        @Override
        public OperatorID getOperatorId() {
            return new OperatorID();
        }

        @Override
        public OperatorCoordinatorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void failJob(Throwable cause) {}

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return getClass().getClassLoader();
        }

        @Override
        public CoordinatorStore getCoordinatorStore() {
            return null;
        }

        @Override
        public boolean isConcurrentExecutionAttemptsSupported() {
            return false;
        }

        @Override
        public CheckpointCoordinator getCheckpointCoordinator() {
            return null;
        }
    }
}

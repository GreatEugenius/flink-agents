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
    void candidateIsAnnouncedOnlyAfterItsCheckpointCompletes() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        FakeGateway gateway = ready(coordinator, 0);

        String submittedJson = json(plan("actionA"));
        String canonicalJson = PlanIds.canonicalize(submittedJson);
        PlanUpdateResponse response = submit(coordinator, submittedJson);
        assertThat(response.planVersion).isEqualTo(1L);
        assertThat(response.planId).isEqualTo(PlanIds.planIdOf(canonicalJson));
        // The candidate is volatile and nothing goes out before every subtask crossed K.
        assertThat(gateway.sentEvents).isEmpty();

        CompletableFuture<byte[]> snapshot = new CompletableFuture<>();
        coordinator.checkpointCoordinator(10L, snapshot);
        CoordinatorState persisted = state(snapshot);
        assertThat(persisted.active).isNull();
        assertThat(OBJECT_MAPPER.readTree(snapshot.get()).has("candidate")).isFalse();
        assertThat(OBJECT_MAPPER.readTree(snapshot.get()).has("staged")).isFalse();
        assertThat(gateway.sentEvents).isEmpty();

        // K completed globally: announce to every subtask.
        coordinator.notifyCheckpointComplete(10L);
        assertThat(gateway.sentEvents).hasSize(1);
        PlanUpdateEvent event = (PlanUpdateEvent) gateway.sentEvents.get(0);
        assertThat(event.planVersion).isEqualTo(1L);
        assertThat(event.planId).isEqualTo(response.planId);
        assertThat(event.canonicalPlanJson).isEqualTo(canonicalJson);
    }

    @Test
    void completedCheckpointBeforeSubmissionDoesNotQualifyLaterCandidate() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        FakeGateway gateway = ready(coordinator, 0);

        completeCheckpoint(coordinator, 9L);
        submit(coordinator, json(plan("actionA")));

        assertThat(gateway.sentEvents).isEmpty();
        completeCheckpoint(coordinator, 10L);
        assertThat(gateway.sentEvents).hasSize(1);
    }

    @Test
    void activationIsRecordedByTheCheckpointAfterTheAnnounce() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        ready(coordinator, 0);
        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L);

        // The checkpoint after the announce records active = candidate: restoring it implies every
        // subtask switched at its barrier.
        CompletableFuture<byte[]> snapshot = new CompletableFuture<>();
        coordinator.checkpointCoordinator(11L, snapshot);
        CoordinatorState persisted = state(snapshot);
        assertThat(persisted.active.planVersion).isEqualTo(1L);
        assertThat(coordinator.getActiveForTesting()).isNull();
        assertThat(coordinator.getCandidateForTesting().planVersion).isEqualTo(1L);
        coordinator.notifyCheckpointComplete(11L);
        assertThat(coordinator.getActiveForTesting().planVersion).isEqualTo(1L);
        assertThat(coordinator.getCandidateForTesting()).isNull();

        // The next update is sequenced on top of the activated version.
        assertThat(submit(coordinator, json(plan("actionB"))).planVersion).isEqualTo(2L);
    }

    @Test
    void updatesAreRejectedWhileCandidatePendingOrCheckpointInFlight() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        ready(coordinator, 0);
        String planJson = json(plan("actionA"));
        submit(coordinator, planJson);

        // One in-flight update at a time.
        assertThatThrownBy(() -> submit(coordinator, planJson))
                .rootCause()
                .hasMessageContaining("candidate and not yet effective");

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
    void failedAnnounceDeliveryFailsTheJob() throws Exception {
        FakeContext context = new FakeContext();
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(context);
        FakeGateway gateway = ready(coordinator, 0);
        gateway.autoAck = false;

        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L);
        CompletableFuture<byte[]> activation = new CompletableFuture<>();
        coordinator.checkpointCoordinator(11L, activation);

        gateway.failAll(new RuntimeException("delivery failed"));

        assertThat(activation).isCompletedExceptionally();
        assertThat(context.jobFailures).hasSize(1);
        assertThat(context.jobFailures.get(0)).hasMessageContaining("planVersion 1");
    }

    @Test
    void abortedCandidateCheckpointIsRetriedByTheNextCheckpoint() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        FakeGateway gateway = ready(coordinator, 0);

        submit(coordinator, json(plan("actionA")));
        coordinator.checkpointCoordinator(10L, new CompletableFuture<>());
        coordinator.notifyCheckpointAborted(10L);
        assertThat(gateway.sentEvents).isEmpty();

        CompletableFuture<byte[]> retry = new CompletableFuture<>();
        coordinator.checkpointCoordinator(11L, retry);
        assertThat(gateway.sentEvents).isEmpty();
        coordinator.notifyCheckpointComplete(11L);
        assertThat(gateway.sentEvents).hasSize(1);
    }

    @Test
    void completionOfAnUnknownCheckpointDoesNotAnnounceTheCandidate() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        FakeGateway gateway = ready(coordinator, 0);
        submit(coordinator, json(plan("actionA")));

        coordinator.notifyCheckpointComplete(10L);

        assertThat(gateway.sentEvents).isEmpty();
    }

    @Test
    void globalResetDropsAnnouncedCandidateBecauseItIsNotCheckpointed() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        FakeGateway gateway = ready(coordinator, 0);

        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L); // announce v1
        completeCheckpoint(coordinator, 11L); // activate v1

        submit(coordinator, json(plan("actionB")));
        CompletableFuture<byte[]> checkpointK = new CompletableFuture<>();
        coordinator.checkpointCoordinator(12L, checkpointK);
        assertThat(state(checkpointK).active.planVersion).isEqualTo(1L);
        coordinator.notifyCheckpointComplete(12L); // announce volatile v2
        assertThat(gateway.sentEvents).hasSize(2);

        PlanUpdateCoordinator restored = new PlanUpdateCoordinator(new FakeContext());
        restored.resetToCheckpoint(12L, checkpointK.get());
        FakeGateway restoredGateway = ready(restored, 0);

        assertThat(restoredGateway.sentEvents).isEmpty();
        assertThat(restored.getCandidateForTesting()).isNull();
        assertThat(submit(restored, json(plan("actionB"))).planVersion).isEqualTo(2L);
    }

    @Test
    void delayedDeliveryFailureFromBeforeResetDoesNotFailANewCandidate() throws Exception {
        FakeContext context = new FakeContext();
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(context);
        FakeGateway oldGateway = ready(coordinator, 0);
        oldGateway.autoAck = false;

        submit(coordinator, json(plan("actionA")));
        CompletableFuture<byte[]> checkpointK = new CompletableFuture<>();
        coordinator.checkpointCoordinator(10L, checkpointK);
        coordinator.notifyCheckpointComplete(10L);
        CompletableFuture<byte[]> abandonedActivation = new CompletableFuture<>();
        coordinator.checkpointCoordinator(11L, abandonedActivation);

        coordinator.resetToCheckpoint(10L, checkpointK.get());
        ready(coordinator, 0);
        submit(coordinator, json(plan("actionB")));

        oldGateway.failAll(new RuntimeException("late delivery failure"));

        assertThat(abandonedActivation).isCompletedExceptionally();
        assertThat(context.jobFailures).isEmpty();
        assertThat(coordinator.getCandidateForTesting().planVersion).isEqualTo(1L);
    }

    @Test
    void synchronousAnnounceFailureFailsTheJob() throws Exception {
        FakeContext context = new FakeContext();
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(context);
        FakeGateway gateway = ready(coordinator, 0);
        gateway.sendFailure = new RuntimeException("send failed");

        submit(coordinator, json(plan("actionA")));
        CompletableFuture<byte[]> checkpointK = new CompletableFuture<>();
        coordinator.checkpointCoordinator(10L, checkpointK);
        coordinator.notifyCheckpointComplete(10L);

        assertThat(context.jobFailures).hasSize(1);
        assertThat(context.jobFailures.get(0)).hasMessageContaining("planVersion 1");
    }

    @Test
    void attemptFailureWhileCandidateExistsFailsTheJob() throws Exception {
        FakeContext context = new FakeContext();
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(context);
        ready(coordinator, 0);
        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L);

        coordinator.executionAttemptFailed(0, 0, new RuntimeException("prepare failed"));

        assertThat(context.jobFailures).hasSize(1);
        assertThat(context.jobFailures.get(0)).hasMessageContaining("planVersion 1");
    }

    @Test
    void replacementAttemptAfterAnnounceFailsTheJob() throws Exception {
        FakeContext context = new FakeContext();
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(context);
        ready(coordinator, 0);
        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L);

        coordinator.executionAttemptReady(0, 1, new FakeGateway(0));

        assertThat(context.jobFailures).hasSize(1);
        assertThat(context.jobFailures.get(0)).hasMessageContaining("attempt 1");
    }

    @Test
    void attemptFailureDuringActivationCheckpointFailsTheJob() throws Exception {
        FakeContext context = new FakeContext();
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(context);
        ready(coordinator, 0);
        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L);

        CompletableFuture<byte[]> activation = new CompletableFuture<>();
        coordinator.checkpointCoordinator(11L, activation);
        assertThat(activation).isDone();
        coordinator.executionAttemptFailed(0, 0, new RuntimeException("activation failed"));

        assertThat(context.jobFailures).hasSize(1);
        assertThat(context.jobFailures.get(0)).hasMessageContaining("planVersion 1");
    }

    @Test
    void abortedActivationCheckpointFailsTheJob() throws Exception {
        FakeContext context = new FakeContext();
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(context);
        ready(coordinator, 0);
        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L);

        coordinator.checkpointCoordinator(11L, new CompletableFuture<>());
        coordinator.notifyCheckpointAborted(11L);

        assertThat(context.jobFailures).hasSize(1);
        assertThat(context.jobFailures.get(0)).hasMessageContaining("checkpoint 11");
    }

    @Test
    void attemptFailureAfterActivationCheckpointCompletesUsesNormalFailover() throws Exception {
        FakeContext context = new FakeContext();
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(context);
        ready(coordinator, 0);
        submit(coordinator, json(plan("actionA")));
        completeCheckpoint(coordinator, 10L);
        completeCheckpoint(coordinator, 11L);

        coordinator.executionAttemptFailed(0, 0, new RuntimeException("unrelated failure"));

        assertThat(context.jobFailures).isEmpty();
    }

    @Test
    void restoreWithEmptyStateClearsTheGates() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        coordinator.checkpointCoordinator(1L, new CompletableFuture<>());
        coordinator.resetToCheckpoint(1L, new byte[0]);
        ready(coordinator, 0);
        assertThat(submit(coordinator, json(plan("actionA"))).planVersion).isEqualTo(1L);
    }

    @Test
    void restoreIgnoresTheLegacyStagedField() throws Exception {
        PlanUpdateCoordinator coordinator = new PlanUpdateCoordinator(new FakeContext());
        Map<String, Object> legacyState = new HashMap<>();
        legacyState.put("active", new PlanUpdateCoordinator.PlanRecord(4L, "active", "{}"));
        legacyState.put("staged", new PlanUpdateCoordinator.PlanRecord(5L, "staged", "{}"));

        coordinator.resetToCheckpoint(4L, OBJECT_MAPPER.writeValueAsBytes(legacyState));
        ready(coordinator, 0);

        assertThat(coordinator.getActiveForTesting().planVersion).isEqualTo(4L);
        assertThat(coordinator.getCandidateForTesting()).isNull();
        assertThat(submit(coordinator, json(plan("actionA"))).planVersion).isEqualTo(5L);
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
        RuntimeException sendFailure;

        FakeGateway(int subtask) {
            this.subtask = subtask;
        }

        @Override
        public CompletableFuture<Acknowledge> sendEvent(OperatorEvent event) {
            if (sendFailure != null) {
                throw sendFailure;
            }
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

        void failAll(Throwable failure) {
            unacked.forEach(f -> f.completeExceptionally(failure));
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

    /** Minimal context that also records requests to escalate an update failure globally. */
    private static final class FakeContext implements OperatorCoordinator.Context {
        final List<Throwable> jobFailures = new ArrayList<>();

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
        public void failJob(Throwable cause) {
            jobFailures.add(cause);
        }

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

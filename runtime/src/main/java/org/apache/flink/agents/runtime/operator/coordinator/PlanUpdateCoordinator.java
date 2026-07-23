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
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateEvent;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateRequest;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateResponse;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * JM-side coordinator implementing the <b>two-phase plan switch</b>: the single point that decides
 * both <i>which</i> plan is next and <i>at which checkpoint</i> every subtask switches to it.
 *
 * <p><b>Phase 1 — stage.</b> An accepted update only becomes {@code staged}: nothing is sent to the
 * subtasks yet. The next checkpoint snapshot persists {@code {active, staged}}; once that
 * checkpoint <b>completes</b> (the decision is durable), the staged plan is announced to every
 * subtask. Announcing strictly after the staging checkpoint completed means every subtask receives
 * the instruction after its own snapshot of that checkpoint — inside the same checkpoint epoch.
 *
 * <p><b>Phase 2 — switch.</b> Subtasks apply the standing rule "prepare on receipt, switch at the
 * next barrier". The coordinator guarantees they all resolve this to the <i>same</i> barrier: its
 * snapshot for the following checkpoint completes only after every announce delivery is confirmed,
 * and Flink injects that checkpoint's barriers at the sources only after all coordinator snapshots
 * complete — so "instruction received" causally precedes "any such barrier exists". That snapshot
 * also records the activation ({@code active = staged}): restoring it implies the switch happened
 * at its barrier.
 *
 * <p>While a staged plan awaits activation (and while any checkpoint is in flight) new updates are
 * rejected — clients retry. One in-flight update at a time keeps the state machine single-point and
 * unambiguous. Restores re-announce a restored staged plan on {@code executionAttemptReady};
 * subtask-side planVersion checks make every re-delivery idempotent.
 *
 * <p>Assumes no concurrent checkpoints ({@code maxConcurrentCheckpoints = 1}, Flink's default).
 */
public class PlanUpdateCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PlanUpdateCoordinator.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Context context;
    private final Map<Integer, SubtaskGateway> gateways = new ConcurrentHashMap<>();

    // Two-phase state: the plan every subtask runs (null = bootstrap plan from the job graph) and
    // the accepted-but-not-yet-effective update. Persisted in every coordinator snapshot.
    @Nullable private volatile PlanRecord active;
    @Nullable private volatile PlanRecord staged;

    // Whether the staged plan has been announced to the subtasks. Set only after the staging
    // checkpoint completed (the decision is durable). Not persisted: a restored staged plan was by
    // definition durable, so restore re-announces (see resetToCheckpoint/executionAttemptReady).
    private volatile boolean stagedAnnounced;

    // Delivery confirmations of announced events. The next checkpoint's snapshot completes only
    // after these are done, which fences "instruction received" before "next barrier injected".
    private final List<CompletableFuture<?>> pendingDeliveries = new CopyOnWriteArrayList<>();

    private final Set<Long> checkpointsInFlight = ConcurrentHashMap.newKeySet();

    PlanUpdateCoordinator(Context context) {
        this.context = context;
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        if (!checkpointsInFlight.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "A checkpoint is in progress; the AgentPlan update is rejected to keep "
                                    + "the switch decision unambiguous. Retry shortly."));
        }
        if (staged != null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "A previous AgentPlan update is staged and not yet effective; one "
                                    + "update is processed at a time. Retry after it activates."));
        }
        String rawJson = ((PlanUpdateRequest) request).agentPlanJson;
        String canonicalJson;
        String planId;
        try {
            canonicalJson = PlanIds.canonicalize(rawJson);
            // Static validation: content errors are rejected before staging.
            OBJECT_MAPPER.readValue(canonicalJson, AgentPlan.class);
            planId = PlanIds.planIdOf(canonicalJson);
        } catch (Exception e) {
            LOG.warn("Rejecting AgentPlan update at the coordinator.", e);
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                            "Rejected AgentPlan update: " + e.getMessage(), e));
        }

        PlanRecord next;
        synchronized (this) {
            long nextVersion = active == null ? 1L : active.planVersion + 1;
            next = new PlanRecord(nextVersion, planId, canonicalJson);
            staged = next;
            stagedAnnounced = false;
        }
        LOG.info(
                "Staged AgentPlan update with planVersion {}; it will be announced once the next "
                        + "checkpoint persists it and switched at the checkpoint after that.",
                next.planVersion);
        return CompletableFuture.completedFuture(
                new PlanUpdateResponse(next.planVersion, next.planId, gateways.size()));
    }

    /**
     * Coordinator snapshot: (a) fences the next barrier behind all announce deliveries, (b) records
     * the activation if the staged plan was already announced — a restore of this checkpoint
     * implies every subtask switched at its barrier — and (c) persists {@code {active, staged}}.
     */
    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        checkpointsInFlight.add(checkpointId);
        List<CompletableFuture<?>> outstanding = new ArrayList<>(pendingDeliveries);
        pendingDeliveries.clear();
        CompletableFuture.allOf(outstanding.toArray(new CompletableFuture<?>[0]))
                .whenComplete(
                        (ignored, deliveryFailure) -> {
                            if (deliveryFailure != null) {
                                // An unconfirmed announce means some subtask may not know the
                                // switch point; failing this checkpoint keeps the invariant
                                // (Flink separately triggers failover for lost events).
                                result.completeExceptionally(deliveryFailure);
                                return;
                            }
                            try {
                                synchronized (this) {
                                    if (staged != null && stagedAnnounced) {
                                        LOG.info(
                                                "Activating AgentPlan planVersion {} at checkpoint"
                                                        + " {}: all subtasks switch at its barrier.",
                                                staged.planVersion,
                                                checkpointId);
                                        active = staged;
                                        staged = null;
                                        stagedAnnounced = false;
                                    }
                                    result.complete(
                                            OBJECT_MAPPER.writeValueAsBytes(
                                                    new CoordinatorState(active, staged)));
                                }
                            } catch (Exception e) {
                                result.completeExceptionally(e);
                            }
                        });
    }

    /** The staging checkpoint completed: the staged decision is durable — announce it. */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        checkpointsInFlight.remove(checkpointId);
        announceStagedIfDurable();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        checkpointsInFlight.remove(checkpointId);
        // The aborted checkpoint persisted nothing: a staged plan stays unannounced and rides
        // along into the next checkpoint's snapshot.
    }

    private synchronized void announceStagedIfDurable() {
        if (staged == null || stagedAnnounced) {
            return;
        }
        int parallelism = context.currentParallelism();
        for (int i = 0; i < parallelism; i++) {
            SubtaskGateway gateway = gateways.get(i);
            if (gateway != null) {
                pendingDeliveries.add(gateway.sendEvent(toEvent(staged)));
            }
        }
        stagedAnnounced = true;
        LOG.info(
                "Announced staged AgentPlan planVersion {} to {} subtasks; they switch at the "
                        + "next checkpoint barrier.",
                staged.planVersion,
                parallelism);
    }

    /**
     * Recovery backfill (initial start, failover and rescale all take this path): re-announce a
     * durable staged plan so an interrupted switch resumes. The active plan is deliberately NOT
     * resent — every subtask recovers it from its own union state, and the two-phase protocol keeps
     * that state equal to {@code active} in every completed checkpoint. Flink never replays
     * operator events — this is the only redelivery mechanism.
     */
    @Override
    public void executionAttemptReady(int subtask, int attempt, SubtaskGateway gateway) {
        gateways.put(subtask, gateway);
        PlanRecord currentStaged = staged;
        if (currentStaged != null && stagedAnnounced) {
            pendingDeliveries.add(gateway.sendEvent(toEvent(currentStaged)));
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attempt, Throwable reason) {
        gateways.remove(subtask);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] data) {
        checkpointsInFlight.clear();
        pendingDeliveries.clear();
        try {
            CoordinatorState state =
                    (data == null || data.length == 0)
                            ? new CoordinatorState(null, null)
                            : OBJECT_MAPPER.readValue(data, CoordinatorState.class);
            synchronized (this) {
                active = state.active;
                staged = state.staged;
                // A restored staged plan was durable by definition; mark it announced so
                // executionAttemptReady re-delivers it and the interrupted switch resumes.
                stagedAnnounced = staged != null;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to restore coordinator plan state.", e);
        }
    }

    private static PlanUpdateEvent toEvent(PlanRecord plan) {
        return new PlanUpdateEvent(plan.planVersion, plan.planId, plan.canonicalPlanJson);
    }

    @Override
    public void start() {}

    @Override
    public void close() {}

    @Override
    public void handleEventFromOperator(int subtask, int attempt, OperatorEvent event) {}

    @Override
    public void subtaskReset(int subtask, long checkpointId) {}

    @VisibleForTesting
    @Nullable
    PlanRecord getActiveForTesting() {
        return active;
    }

    @VisibleForTesting
    @Nullable
    PlanRecord getStagedForTesting() {
        return staged;
    }

    /** One immutable plan record: {@code planVersion + content identity + full content}. */
    public static final class PlanRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public long planVersion;
        public String planId;
        public String canonicalPlanJson;

        public PlanRecord() {}

        public PlanRecord(long planVersion, String planId, String canonicalPlanJson) {
            this.planVersion = planVersion;
            this.planId = planId;
            this.canonicalPlanJson = canonicalPlanJson;
        }
    }

    /** Checkpointed coordinator state: the running plan and the not-yet-effective update. */
    public static final class CoordinatorState implements Serializable {
        private static final long serialVersionUID = 1L;
        @Nullable public PlanRecord active;
        @Nullable public PlanRecord staged;

        public CoordinatorState() {}

        public CoordinatorState(@Nullable PlanRecord active, @Nullable PlanRecord staged) {
            this.active = active;
            this.staged = staged;
        }
    }

    /** Factory wired into {@code CoordinatedOperatorFactory.getCoordinatorProvider}. */
    public static class Provider implements OperatorCoordinator.Provider {
        private static final long serialVersionUID = 1L;
        private final OperatorID operatorId;

        public Provider(OperatorID operatorId) {
            this.operatorId = operatorId;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new PlanUpdateCoordinator(context);
        }
    }
}

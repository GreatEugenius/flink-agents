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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
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
 * <p><b>Phase 1 — establish the lower bound.</b> An accepted update only becomes an in-memory
 * {@code candidate}: nothing is sent to the subtasks yet. The next completed checkpoint K still
 * snapshots only the active plan. The candidate is announced after K completes, so every subtask
 * receives the instruction after its own K snapshot.
 *
 * <p><b>Phase 2 — switch.</b> Subtasks apply the standing rule "prepare on receipt, switch at the
 * next barrier". The coordinator guarantees they all resolve this to the <i>same</i> barrier: its
 * snapshot for the following checkpoint completes only after every announce delivery is confirmed,
 * and Flink injects that checkpoint's barriers at the sources only after all coordinator snapshots
 * complete — so "instruction received" causally precedes "any such barrier exists". That snapshot
 * records the candidate as active: restoring it implies the switch happened at its barrier.
 *
 * <p>Only the active plan is checkpointed. The candidate and all delivery bookkeeping are volatile:
 * a global recovery before the activation checkpoint completes deliberately abandons the update. A
 * subtask failure while a candidate is in flight is escalated through {@link Context#failJob} so
 * every task and this coordinator recover from the same completed checkpoint. While a candidate
 * awaits activation (and while any checkpoint is in flight) new updates are rejected.
 *
 * <p>Assumes no concurrent checkpoints ({@code maxConcurrentCheckpoints = 1}, Flink's default).
 */
public class PlanUpdateCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PlanUpdateCoordinator.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final long NO_ACTIVATION_CHECKPOINT = -1L;

    private final Context context;
    private final Map<Integer, SubtaskGateway> gateways = new ConcurrentHashMap<>();

    // Durable state: the plan in the latest completed activation checkpoint. Null means the
    // bootstrap plan from the JobGraph.
    @Nullable private volatile PlanRecord active;

    // Volatile transaction state. K completes before candidatePassedCheckpoint becomes true and
    // the event is announced. The candidate remains present through K+1 so a barrier-time failure
    // can still be recognized and escalated to global recovery.
    @Nullable private volatile PlanRecord candidate;
    private volatile boolean candidatePassedCheckpoint;
    private volatile long activationCheckpointId = NO_ACTIVATION_CHECKPOINT;
    private volatile boolean candidateFailureEscalated;

    // Delivery confirmations of announced events. The next checkpoint's snapshot completes only
    // after these are done, which fences "instruction received" before "next barrier injected".
    private final List<CompletableFuture<?>> pendingDeliveries = new CopyOnWriteArrayList<>();

    private final Set<Long> checkpointsInFlight = ConcurrentHashMap.newKeySet();

    // Records which candidate was present when checkpoint K took its coordinator snapshot. This
    // makes the lower-bound qualification explicit instead of letting a later request borrow an
    // already-completed checkpoint. Accessed only while holding this coordinator's monitor.
    private final Map<Long, PlanRecord> lowerBoundCandidates = new HashMap<>();

    PlanUpdateCoordinator(Context context) {
        this.context = context;
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        IllegalStateException rejection = currentRequestRejection();
        if (rejection != null) {
            return CompletableFuture.failedFuture(rejection);
        }
        String rawJson = ((PlanUpdateRequest) request).agentPlanJson;
        String canonicalJson;
        String planId;
        try {
            canonicalJson = PlanIds.canonicalize(rawJson);
            // Static validation: content errors are rejected before accepting a candidate.
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
            // Validation deliberately happens outside the lock. Re-check under the same lock used
            // by checkpointCoordinator so a request cannot slip behind a checkpoint snapshot and
            // then incorrectly treat that checkpoint as K.
            rejection = currentRequestRejection();
            if (rejection != null) {
                return CompletableFuture.failedFuture(rejection);
            }
            long nextVersion = active == null ? 1L : active.planVersion + 1;
            next = new PlanRecord(nextVersion, planId, canonicalJson);
            candidate = next;
            candidatePassedCheckpoint = false;
            activationCheckpointId = NO_ACTIVATION_CHECKPOINT;
            candidateFailureEscalated = false;
        }
        LOG.info(
                "Accepted AgentPlan candidate with planVersion {}; it will be announced after all "
                        + "subtasks complete the next checkpoint and switched at the checkpoint "
                        + "after that.",
                next.planVersion);
        return CompletableFuture.completedFuture(
                new PlanUpdateResponse(next.planVersion, next.planId, gateways.size()));
    }

    /**
     * Coordinator snapshot: (a) fences K+1 behind all announce deliveries, (b) records the
     * candidate as active in K+1 — a restore of that checkpoint implies every subtask switched at
     * its barrier — and (c) persists only the active plan. K still records the old active plan.
     */
    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        final List<CompletableFuture<?>> outstanding;
        final boolean activationSnapshot;
        final PlanRecord checkpointCandidate;
        synchronized (this) {
            checkpointsInFlight.add(checkpointId);
            outstanding = new ArrayList<>(pendingDeliveries);
            checkpointCandidate = candidate;
            activationSnapshot = checkpointCandidate != null && candidatePassedCheckpoint;
            if (checkpointCandidate != null && !activationSnapshot && !candidateFailureEscalated) {
                lowerBoundCandidates.put(checkpointId, checkpointCandidate);
            }
        }
        CompletableFuture.allOf(outstanding.toArray(new CompletableFuture<?>[0]))
                .whenComplete(
                        (ignored, deliveryFailure) -> {
                            if (deliveryFailure != null) {
                                if (activationSnapshot) {
                                    failCurrentUpdateGlobally(
                                            "Failed to deliver the AgentPlan candidate to every "
                                                    + "subtask.",
                                            deliveryFailure,
                                            checkpointCandidate,
                                            checkpointId);
                                }
                                result.completeExceptionally(deliveryFailure);
                                return;
                            }
                            try {
                                synchronized (this) {
                                    if (!checkpointsInFlight.contains(checkpointId)) {
                                        result.completeExceptionally(
                                                new IllegalStateException(
                                                        "Checkpoint "
                                                                + checkpointId
                                                                + " was aborted before the "
                                                                + "coordinator snapshot completed."));
                                        return;
                                    }

                                    PlanRecord snapshotActive = active;
                                    if (activationSnapshot) {
                                        if (candidate != checkpointCandidate
                                                || !candidatePassedCheckpoint) {
                                            throw new IllegalStateException(
                                                    "AgentPlan candidate changed while "
                                                            + "checkpoint "
                                                            + checkpointId
                                                            + " was waiting for delivery.");
                                        }
                                        LOG.info(
                                                "Activating AgentPlan planVersion {} at checkpoint"
                                                        + " {}: all subtasks switch at its barrier.",
                                                candidate.planVersion,
                                                checkpointId);
                                        activationCheckpointId = checkpointId;
                                        snapshotActive = checkpointCandidate;
                                        pendingDeliveries.removeAll(outstanding);
                                    }
                                    result.complete(
                                            OBJECT_MAPPER.writeValueAsBytes(
                                                    new CoordinatorState(snapshotActive)));
                                }
                            } catch (Exception e) {
                                if (activationSnapshot) {
                                    failCurrentUpdateGlobally(
                                            "Failed to snapshot the activating AgentPlan "
                                                    + "candidate.",
                                            e,
                                            checkpointCandidate,
                                            checkpointId);
                                }
                                result.completeExceptionally(e);
                            }
                        });
    }

    /** Completes K's lower-bound fence or commits the activation checkpoint K+1. */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        PlanRecord candidateToAnnounce;
        int parallelism = 0;
        String failureMessage = null;
        RuntimeException failureCause = null;
        synchronized (this) {
            if (!checkpointsInFlight.remove(checkpointId)) {
                return;
            }
            PlanRecord lowerBoundCandidate = lowerBoundCandidates.remove(checkpointId);
            if (activationCheckpointId == checkpointId) {
                if (candidate == null) {
                    throw new IllegalStateException(
                            "Activation checkpoint completed without an AgentPlan candidate.");
                }
                active = candidate;
                candidate = null;
                candidatePassedCheckpoint = false;
                activationCheckpointId = NO_ACTIVATION_CHECKPOINT;
                candidateFailureEscalated = false;
                return;
            }

            // Qualify the candidate while holding the same monitor used by request admission and
            // checkpointCoordinator. Otherwise a request accepted immediately after a checkpoint
            // completed could accidentally borrow that already-finished checkpoint as K.
            if (lowerBoundCandidate == null
                    || candidate != lowerBoundCandidate
                    || candidatePassedCheckpoint
                    || candidateFailureEscalated) {
                return;
            }
            candidatePassedCheckpoint = true;
            candidateToAnnounce = candidate;
            try {
                parallelism = context.currentParallelism();
                if (gateways.size() != parallelism) {
                    failureMessage =
                            "Cannot announce AgentPlan candidate planVersion "
                                    + candidate.planVersion
                                    + ": expected "
                                    + parallelism
                                    + " ready subtasks but found "
                                    + gateways.size()
                                    + '.';
                } else {
                    PlanUpdateEvent event = toEvent(candidateToAnnounce);
                    for (int i = 0; i < parallelism; i++) {
                        SubtaskGateway gateway = gateways.get(i);
                        pendingDeliveries.add(gateway.sendEvent(event));
                    }
                }
            } catch (RuntimeException e) {
                failureMessage = "Failed to announce the AgentPlan candidate to every subtask.";
                failureCause = e;
            }
        }
        if (failureMessage != null) {
            failCurrentUpdateGlobally(
                    failureMessage, failureCause, candidateToAnnounce, NO_ACTIVATION_CHECKPOINT);
            return;
        }
        LOG.info(
                "Announced AgentPlan candidate planVersion {} to {} subtasks; they switch at the "
                        + "next checkpoint barrier.",
                candidateToAnnounce.planVersion,
                parallelism);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        PlanRecord candidateAtAbort = null;
        synchronized (this) {
            checkpointsInFlight.remove(checkpointId);
            lowerBoundCandidates.remove(checkpointId);
            if (activationCheckpointId == checkpointId) {
                candidateAtAbort = candidate;
            }
        }
        if (candidateAtAbort != null) {
            failCurrentUpdateGlobally(
                    "AgentPlan activation checkpoint "
                            + checkpointId
                            + " was aborted after its barrier could have switched subtasks.",
                    null,
                    candidateAtAbort,
                    NO_ACTIVATION_CHECKPOINT);
        }
    }

    /**
     * Registers the current attempt. The active plan is never sent: every subtask restores it from
     * union state. A candidate is not replayed after recovery; a global reset discards it. Once a
     * candidate was announced, another ready attempt means the attempt set changed during the
     * update and therefore requires global recovery.
     */
    @Override
    public void executionAttemptReady(int subtask, int attempt, SubtaskGateway gateway) {
        PlanRecord announcedCandidate = null;
        synchronized (this) {
            gateways.put(subtask, gateway);
            if (candidate != null && candidatePassedCheckpoint && !candidateFailureEscalated) {
                announcedCandidate = candidate;
            }
        }
        if (announcedCandidate != null) {
            failCurrentUpdateGlobally(
                    "Subtask "
                            + subtask
                            + " attempt "
                            + attempt
                            + " became ready after the AgentPlan candidate was announced.",
                    null,
                    announcedCandidate,
                    NO_ACTIVATION_CHECKPOINT);
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attempt, Throwable reason) {
        PlanRecord failedCandidate;
        synchronized (this) {
            gateways.remove(subtask);
            failedCandidate = candidate;
        }
        if (failedCandidate != null) {
            failCurrentUpdateGlobally(
                    "Subtask "
                            + subtask
                            + " attempt "
                            + attempt
                            + " failed while applying the AgentPlan candidate.",
                    reason,
                    failedCandidate,
                    NO_ACTIVATION_CHECKPOINT);
        }
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] data) {
        synchronized (this) {
            checkpointsInFlight.clear();
            pendingDeliveries.clear();
            gateways.clear();
            lowerBoundCandidates.clear();
            try {
                CoordinatorState state =
                        (data == null || data.length == 0)
                                ? new CoordinatorState(null)
                                : OBJECT_MAPPER.readValue(data, CoordinatorState.class);
                active = state.active;
                candidate = null;
                candidatePassedCheckpoint = false;
                activationCheckpointId = NO_ACTIVATION_CHECKPOINT;
                candidateFailureEscalated = false;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to restore coordinator plan state.", e);
            }
        }
    }

    private synchronized @Nullable IllegalStateException currentRequestRejection() {
        if (!checkpointsInFlight.isEmpty()) {
            return new IllegalStateException(
                    "A checkpoint is in progress; the AgentPlan update is rejected to keep the "
                            + "switch decision unambiguous. Retry shortly.");
        }
        if (candidate != null) {
            return new IllegalStateException(
                    "A previous AgentPlan update is a candidate and not yet effective; one update "
                            + "is processed at a time. Retry after it activates.");
        }
        return null;
    }

    private void failCurrentUpdateGlobally(
            String message,
            @Nullable Throwable cause,
            @Nullable PlanRecord expectedCandidate,
            long expectedCheckpointId) {
        final long planVersion;
        synchronized (this) {
            if (candidate == null
                    || candidateFailureEscalated
                    || (expectedCandidate != null && candidate != expectedCandidate)
                    || (expectedCheckpointId != NO_ACTIVATION_CHECKPOINT
                            && !checkpointsInFlight.contains(expectedCheckpointId))) {
                return;
            }
            candidateFailureEscalated = true;
            planVersion = candidate.planVersion;
        }

        String failureMessage =
                message + " Failing the job for AgentPlan planVersion " + planVersion + '.';
        FlinkException failure =
                cause == null
                        ? new FlinkException(failureMessage)
                        : new FlinkException(failureMessage, cause);
        LOG.warn(failureMessage, cause);
        context.failJob(failure);
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
    public void subtaskReset(int subtask, long checkpointId) {
        PlanRecord resetCandidate;
        synchronized (this) {
            resetCandidate = candidate;
        }
        if (resetCandidate != null) {
            failCurrentUpdateGlobally(
                    "Subtask "
                            + subtask
                            + " was reset to checkpoint "
                            + checkpointId
                            + " while an AgentPlan update was in progress.",
                    null,
                    resetCandidate,
                    NO_ACTIVATION_CHECKPOINT);
        }
    }

    @VisibleForTesting
    @Nullable
    PlanRecord getActiveForTesting() {
        return active;
    }

    @VisibleForTesting
    @Nullable
    PlanRecord getCandidateForTesting() {
        return candidate;
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

    /** Checkpointed coordinator state: only the plan committed by a completed checkpoint. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class CoordinatorState implements Serializable {
        private static final long serialVersionUID = 1L;
        @Nullable public PlanRecord active;

        public CoordinatorState() {}

        public CoordinatorState(@Nullable PlanRecord active) {
            this.active = active;
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

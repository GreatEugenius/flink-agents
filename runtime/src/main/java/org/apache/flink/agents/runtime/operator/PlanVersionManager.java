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
import org.apache.flink.agents.plan.AgentConfiguration;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.JavaFunction;
import org.apache.flink.agents.plan.actions.Action;
import org.apache.flink.agents.runtime.ResourceCache;
import org.apache.flink.agents.runtime.operator.coordinator.PlanIds;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The single plan-access entry inside {@link ActionExecutionOperator} for checkpoint-aligned plan
 * switching. At any point in time exactly one plan is live ({@code current}); at most one validated
 * update waits for the next checkpoint barrier ({@code pending}).
 *
 * <p>Lifecycle of an update: {@link #preparePending} runs at event arrival on the mailbox thread —
 * it verifies the wire signature, deserializes the plan, pins the job-level configuration of the
 * bootstrap plan, and proves every Java action loadable through the job's user-code classloader.
 * Python runtime construction is deliberately deferred until the activation barrier. Newest wins:
 * preparing a newer update discards an unswitched older pending. {@link #switchToPending} runs at
 * {@code prepareSnapshotPreBarrier} after the operator drained its in-flight chains and activated
 * the pending runtime.
 *
 * <p>Checkpointed fact: the current {@code (version, planId, canonicalPlanJson)} as union operator
 * state, one record per subtask. On restore every subtask picks the record with the highest
 * planVersion, so after rescale all subtasks converge on the same plan. An empty state means the
 * job never switched and runs the bootstrap plan from the job graph. The pending slot is
 * deliberately not checkpointed: global recovery discards the volatile update and restores only the
 * completed active plan.
 */
class PlanVersionManager {

    private static final Logger LOG = LoggerFactory.getLogger(PlanVersionManager.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final ListStateDescriptor<String> CURRENT_PLAN_STATE_DESCRIPTOR =
            new ListStateDescriptor<>("dynamic-agent-plan-current", Types.STRING);

    private final AgentPlan bootstrapPlan;

    private PlanSlot current;
    @Nullable private PlanSlot pending;

    private Supplier<ClassLoader> userCodeClassLoader;
    private ListState<String> currentPlanState;

    PlanVersionManager(AgentPlan bootstrapPlan) {
        this.bootstrapPlan = bootstrapPlan;
        String bootstrapPlanJson = canonicalJsonOf(bootstrapPlan);
        this.current =
                new PlanSlot(
                        0L, PlanIds.planIdOf(bootstrapPlanJson), bootstrapPlanJson, bootstrapPlan);
    }

    /**
     * Registers/restores the current-plan union state. Must run before {@link #open} and before the
     * operator builds any plan-derived runtime, so {@code open()} sees the restored plan.
     */
    void initializeState(OperatorStateStore stateStore, boolean restored) throws Exception {
        currentPlanState = stateStore.getUnionListState(CURRENT_PLAN_STATE_DESCRIPTOR);
        if (!restored) {
            return;
        }
        // Union state: every subtask sees all subtasks' records; the highest planVersion wins so
        // all subtasks (including new ones after a rescale) converge on the same plan.
        CurrentPlanRecord newest = null;
        for (String record : currentPlanState.get()) {
            CurrentPlanRecord r = OBJECT_MAPPER.readValue(record, CurrentPlanRecord.class);
            if (newest == null || r.version > newest.version) {
                newest = r;
            }
        }
        if (newest != null) {
            checkState(
                    newest.planId != null && !newest.planId.isBlank(),
                    "Restored dynamic AgentPlan state is missing planId.");
            AgentPlan plan = OBJECT_MAPPER.readValue(newest.canonicalPlanJson, AgentPlan.class);
            current = new PlanSlot(newest.version, newest.planId, newest.canonicalPlanJson, plan);
            LOG.info(
                    "Restored dynamic AgentPlan with planVersion {} from checkpoint state.",
                    newest.version);
        }
    }

    /** Writes the current plan record; a never-switched subtask keeps the state empty. */
    void snapshotState() throws Exception {
        currentPlanState.clear();
        if (current.version > 0) {
            currentPlanState.add(
                    OBJECT_MAPPER.writeValueAsString(
                            new CurrentPlanRecord(
                                    current.version, current.planId, current.canonicalPlanJson)));
        }
    }

    /**
     * Late wiring of the user-code classloader (available only from {@code open()}). A restored
     * dynamic plan re-proves that its Java actions are still loadable before it can run.
     */
    void open(Supplier<ClassLoader> userCodeClassLoader) throws Exception {
        this.userCodeClassLoader = userCodeClassLoader;
        validateJavaActionsExecutable(current.plan, userCodeClassLoader.get());
    }

    long currentPlanVersion() {
        return current.version;
    }

    String currentPlanId() {
        return current.planId;
    }

    AgentPlan currentPlan() {
        return current.plan;
    }

    ClassLoader currentClassLoader() {
        return userCodeClassLoader.get();
    }

    ResourceCache currentResourceCache() {
        if (current.resourceCache == null) {
            current.resourceCache =
                    new ResourceCache(
                            current.plan.getResourceProviders(), userCodeClassLoader.get());
        }
        return current.resourceCache;
    }

    boolean hasPending() {
        return pending != null;
    }

    @Nullable
    AgentPlan pendingPlan() {
        return pending == null ? null : pending.plan;
    }

    @Nullable
    ResourceCache pendingResourceCache() {
        return pending == null ? null : pending.resourceCache;
    }

    /** An update is relevant only if newer than both the live plan and the waiting pending. */
    boolean isNewerThanKnown(long version) {
        return version > current.version && (pending == null || version > pending.version);
    }

    /**
     * Validates and prepares an update as the pending plan, replacing (and closing) any older
     * unswitched pending. Failures leave no pending plan and never disturb the current plan.
     */
    void preparePending(long version, String planId, String canonicalPlanJson) throws Exception {
        checkState(
                isNewerThanKnown(version),
                "Update planVersion %s is not newer than the known plans.",
                version);

        discardPending();

        checkState(
                PlanIds.planIdOf(canonicalPlanJson).equals(planId),
                "Received plan JSON does not match its planId %s.",
                planId);
        AgentPlan received = OBJECT_MAPPER.readValue(canonicalPlanJson, AgentPlan.class);
        AgentPlan effective = withBootstrapConfig(received);

        PlanSlot prepared = new PlanSlot(version, planId, canonicalJsonOf(effective), effective);
        try {
            validateJavaActionsExecutable(effective, userCodeClassLoader.get());
            prepared.resourceCache =
                    new ResourceCache(effective.getResourceProviders(), userCodeClassLoader.get());
        } catch (Exception e) {
            prepared.close();
            throw e;
        }
        pending = prepared;
    }

    /** Drops the pending plan and closes resources prepared for it. */
    void discardPending() throws Exception {
        if (pending == null) {
            return;
        }
        PlanSlot discarded = pending;
        pending = null;
        discarded.close();
    }

    /**
     * Swaps the pending plan in as the current one and closes the previous plan's resources. Must
     * only be called after the operator drained all in-flight event chains, so nothing references
     * the old plan anymore.
     */
    void switchToPending() throws Exception {
        checkState(pending != null, "No pending AgentPlan to switch to.");
        PlanSlot old = current;
        current = pending;
        pending = null;
        old.close();
        LOG.info(
                "Switched AgentPlan from planVersion {} to planVersion {} at the checkpoint barrier.",
                old.version,
                current.version);
    }

    /**
     * Releases live-plan Java resources after the barrier drain but before its Python interpreter
     * is closed. The logical current plan is deliberately unchanged until the replacement runtime
     * is ready. If later activation fails, the caller must fail the task rather than continue with
     * this locally dismantled plan.
     */
    void closeCurrentResourcesForSwitch() throws Exception {
        checkState(pending != null, "No pending AgentPlan to switch to.");
        current.close();
    }

    void close() throws Exception {
        Exception firstException = null;
        try {
            if (pending != null) {
                pending.close();
                pending = null;
            }
        } catch (Exception e) {
            firstException = e;
        }
        try {
            current.close();
        } catch (Exception e) {
            if (firstException == null) {
                firstException = e;
            } else {
                firstException.addSuppressed(e);
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * Job-level configuration is pinned at submission: the effective plan keeps the update's
     * actions/routing/resources but carries the bootstrap {@code AgentConfiguration}.
     */
    private AgentPlan withBootstrapConfig(AgentPlan received) {
        Map<String, Object> merged = new HashMap<>(bootstrapPlan.getConfig().getConfData());
        return new AgentPlan(
                received.getActions(),
                received.getActionsByEvent(),
                received.getResourceProviders(),
                new AgentConfiguration(merged));
    }

    /** Canonical JSON of a plan object — the exact form persisted and restored. */
    static String canonicalJsonOf(AgentPlan plan) {
        try {
            return PlanIds.canonicalize(OBJECT_MAPPER.writeValueAsString(plan));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize AgentPlan.", e);
        }
    }

    /** Every Java action's class must be loadable through the plan's classloader. */
    private static void validateJavaActionsExecutable(AgentPlan plan, ClassLoader classLoader)
            throws Exception {
        for (Action action : plan.getActions().values()) {
            if (action.getExec() instanceof JavaFunction) {
                JavaFunction function = (JavaFunction) action.getExec();
                Class.forName(function.getQualName(), false, classLoader);
            }
        }
    }

    /** One plan version and the plan-scoped resources tied to it. */
    private static final class PlanSlot {
        final long version;
        final String planId;
        private final String canonicalPlanJson;
        final AgentPlan plan;
        @Nullable ResourceCache resourceCache;

        PlanSlot(long version, String planId, String canonicalPlanJson, AgentPlan plan) {
            this.version = version;
            checkState(planId != null && !planId.isBlank(), "planId must not be empty.");
            this.planId = planId;
            this.canonicalPlanJson = canonicalPlanJson;
            this.plan = plan;
        }

        void close() throws Exception {
            if (resourceCache != null) {
                resourceCache.close();
                resourceCache = null;
            }
        }
    }

    /** Checkpointed record: the plan the subtask was running when the barrier passed. */
    public static final class CurrentPlanRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public long version;
        public String planId;
        public String canonicalPlanJson;

        public CurrentPlanRecord() {}

        public CurrentPlanRecord(long version, String planId, String canonicalPlanJson) {
            this.version = version;
            this.planId = planId;
            this.canonicalPlanJson = canonicalPlanJson;
        }
    }
}

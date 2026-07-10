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

import java.io.Closeable;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * bootstrap plan (only {@code dynamic-plan.*} keys of the update are honored), builds the
 * plan-scoped artifact classloader (with sha256 verification) and proves every Java action loadable
 * through it. Heavy preparation therefore never blocks the barrier. Newest wins: preparing a newer
 * update discards an unswitched older pending. {@link #switchToPending} runs at {@code
 * prepareSnapshotPreBarrier} after the operator drained its in-flight chains — it only swaps
 * references and closes the previous plan's resources.
 *
 * <p>Checkpointed fact: the current {@code (version, canonicalPlanJson)} as union operator state,
 * one record per subtask. On restore every subtask picks the record with the highest sequence, so
 * after rescale all subtasks converge on the same plan. An empty state means the job never switched
 * and runs the bootstrap plan from the job graph. The pending slot is deliberately not checkpointed
 * — the coordinator re-sends its latest plan to every restarted subtask ({@code
 * executionAttemptReady} backfill).
 */
class PlanVersionManager {

    private static final Logger LOG = LoggerFactory.getLogger(PlanVersionManager.class);

    static final String JAVA_ARTIFACT_PATH_CONFIG = "dynamic-plan.java-artifact.path";
    static final String JAVA_ARTIFACT_SHA256_CONFIG = "dynamic-plan.java-artifact.sha256";

    /** Update-carried config keys honored during the bootstrap-config merge. */
    private static final String PLAN_SCOPED_CONFIG_KEY_PREFIX = "dynamic-plan.";

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
        this.current = new PlanSlot(0L, null, bootstrapPlan);
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
        // Union state: every subtask sees all subtasks' records; the highest sequence wins so all
        // subtasks (including new ones after a rescale) converge on the same plan.
        CurrentPlanRecord newest = null;
        for (String record : currentPlanState.get()) {
            CurrentPlanRecord r = OBJECT_MAPPER.readValue(record, CurrentPlanRecord.class);
            if (newest == null || r.version > newest.version) {
                newest = r;
            }
        }
        if (newest != null) {
            AgentPlan plan = OBJECT_MAPPER.readValue(newest.canonicalPlanJson, AgentPlan.class);
            current = new PlanSlot(newest.version, newest.canonicalPlanJson, plan);
            LOG.info(
                    "Restored dynamic AgentPlan with planVersion {} from checkpoint state.",
                    newest.version);
        }
    }

    /**
     * Writes the current {@code (version, json)}; a never-switched subtask keeps the state empty.
     */
    void snapshotState() throws Exception {
        currentPlanState.clear();
        if (current.version > 0) {
            currentPlanState.add(
                    OBJECT_MAPPER.writeValueAsString(
                            new CurrentPlanRecord(current.version, current.canonicalPlanJson())));
        }
    }

    /**
     * Late wiring of the user-code classloader (available only from {@code open()}). A restored
     * dynamic plan re-creates its artifact classloader here and re-proves executability — without
     * its artifact the plan cannot run, so this fails fast.
     */
    void open(Supplier<ClassLoader> userCodeClassLoader) throws Exception {
        this.userCodeClassLoader = userCodeClassLoader;
        if (current.version > 0 && current.classLoader == null) {
            current.classLoader = createArtifactClassLoader(current.plan);
            validateJavaActionsExecutable(
                    current.plan, current.classLoaderOrDefault(userCodeClassLoader.get()));
        }
    }

    long currentPlanVersion() {
        return current.version;
    }

    AgentPlan currentPlan() {
        return current.plan;
    }

    /** Stable key of the live plan, used to scope per-plan Python runtimes. */
    String currentPlanKey() {
        return planKey(current.version);
    }

    static String planKey(long version) {
        return "plan-" + version;
    }

    ClassLoader currentClassLoader() {
        return current.classLoaderOrDefault(userCodeClassLoader.get());
    }

    ResourceCache currentResourceCache() {
        if (current.resourceCache == null) {
            current.resourceCache =
                    new ResourceCache(current.plan.getResourceProviders(), currentClassLoader());
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
    String pendingPlanKey() {
        return pending == null ? null : planKey(pending.version);
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
     *
     * @return the key of a discarded older pending plan, or {@code null} if there was none. The
     *     caller must release runtime pieces it built for that key (e.g. the Python runtime).
     */
    @Nullable
    String preparePending(long version, String planId, String canonicalPlanJson) throws Exception {
        checkState(
                isNewerThanKnown(version),
                "Update planVersion %s is not newer than the known plans.",
                version);

        String discardedKey = discardPending();

        checkState(
                PlanIds.planIdOf(canonicalPlanJson).equals(planId),
                "Received plan JSON does not match its planId %s.",
                planId);
        AgentPlan received = OBJECT_MAPPER.readValue(canonicalPlanJson, AgentPlan.class);
        AgentPlan effective = withBootstrapConfig(received);

        PlanSlot prepared = new PlanSlot(version, canonicalJsonOf(effective), effective);
        try {
            prepared.classLoader = createArtifactClassLoader(effective);
            validateJavaActionsExecutable(
                    effective, prepared.classLoaderOrDefault(userCodeClassLoader.get()));
            prepared.resourceCache =
                    new ResourceCache(
                            effective.getResourceProviders(),
                            prepared.classLoaderOrDefault(userCodeClassLoader.get()));
        } catch (Exception e) {
            prepared.close();
            throw e;
        }
        pending = prepared;
        return discardedKey;
    }

    /**
     * Drops the pending plan (newest-wins replacement or prepare failure) and closes its resources.
     *
     * @return the discarded plan's key, or {@code null} if there was no pending plan.
     */
    @Nullable
    String discardPending() throws Exception {
        if (pending == null) {
            return null;
        }
        String key = planKey(pending.version);
        PlanSlot discarded = pending;
        pending = null;
        discarded.close();
        return key;
    }

    /**
     * Swaps the pending plan in as the current one and closes the previous plan's resources. Must
     * only be called after the operator drained all in-flight event chains, so nothing references
     * the old plan anymore.
     *
     * @return the replaced plan's key, for the caller to release its runtime pieces.
     */
    String switchToPending() throws Exception {
        checkState(pending != null, "No pending AgentPlan to switch to.");
        PlanSlot old = current;
        current = pending;
        pending = null;
        String oldKey = planKey(old.version);
        old.close();
        LOG.info(
                "Switched AgentPlan from planVersion {} to planVersion {} at the checkpoint barrier.",
                old.version,
                current.version);
        return oldKey;
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
     * actions/routing/resources but carries the bootstrap {@code AgentConfiguration}, except for
     * the update's own {@code dynamic-plan.*} keys (artifact locations and their sha256), which are
     * plan-scoped by nature.
     */
    private AgentPlan withBootstrapConfig(AgentPlan received) {
        Map<String, Object> merged = new HashMap<>(bootstrapPlan.getConfig().getConfData());
        received.getConfig()
                .getConfData()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(PLAN_SCOPED_CONFIG_KEY_PREFIX)) {
                                merged.put(key, value);
                            }
                        });
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

    @Nullable
    private ClassLoader createArtifactClassLoader(AgentPlan plan) throws Exception {
        String artifactPath = plan.getConfig().getStr(JAVA_ARTIFACT_PATH_CONFIG, null);
        if (artifactPath == null || artifactPath.trim().isEmpty()) {
            return null;
        }
        Path path = Path.of(artifactPath);
        checkState(
                Files.isRegularFile(path),
                "Java artifact path %s is not a readable file.",
                artifactPath);
        String expectedSha256 = plan.getConfig().getStr(JAVA_ARTIFACT_SHA256_CONFIG, null);
        if (expectedSha256 != null && !expectedSha256.trim().isEmpty()) {
            String actual = PlanIds.sha256HexOfFile(path);
            checkState(
                    actual.equalsIgnoreCase(expectedSha256.trim()),
                    "Java artifact %s failed sha256 verification: expected %s but was %s.",
                    artifactPath,
                    expectedSha256,
                    actual);
        }
        return new PlanArtifactClassLoader(
                new URL[] {path.toUri().toURL()}, userCodeClassLoader.get());
    }

    /** One plan version and the plan-scoped resources tied to it. */
    private static final class PlanSlot {
        final long version;
        @Nullable private String canonicalPlanJson;
        final AgentPlan plan;
        @Nullable ClassLoader classLoader;
        @Nullable ResourceCache resourceCache;

        PlanSlot(long version, @Nullable String canonicalPlanJson, AgentPlan plan) {
            this.version = version;
            this.canonicalPlanJson = canonicalPlanJson;
            this.plan = plan;
        }

        ClassLoader classLoaderOrDefault(ClassLoader fallback) {
            return classLoader != null ? classLoader : fallback;
        }

        /** Lazily computed for the bootstrap plan: only needed once a snapshot must persist it. */
        String canonicalPlanJson() {
            if (canonicalPlanJson == null) {
                canonicalPlanJson = canonicalJsonOf(plan);
            }
            return canonicalPlanJson;
        }

        void close() throws Exception {
            Exception firstException = null;
            if (resourceCache != null) {
                try {
                    resourceCache.close();
                } catch (Exception e) {
                    firstException = e;
                }
                resourceCache = null;
            }
            if (classLoader instanceof Closeable) {
                try {
                    ((Closeable) classLoader).close();
                } catch (Exception e) {
                    if (firstException == null) {
                        firstException = e;
                    } else {
                        firstException.addSuppressed(e);
                    }
                }
                classLoader = null;
            }
            if (firstException != null) {
                throw firstException;
            }
        }
    }

    /** Checkpointed record: the plan the subtask was running when the barrier passed. */
    public static final class CurrentPlanRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public long version;
        public String canonicalPlanJson;

        public CurrentPlanRecord() {}

        public CurrentPlanRecord(long version, String canonicalPlanJson) {
            this.version = version;
            this.canonicalPlanJson = canonicalPlanJson;
        }
    }

    private static final class PlanArtifactClassLoader extends URLClassLoader {
        static {
            registerAsParallelCapable();
        }

        private PlanArtifactClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    if (isParentFirstClass(name)) {
                        loaded = super.loadClass(name, false);
                    } else {
                        try {
                            loaded = findClass(name);
                        } catch (ClassNotFoundException ignored) {
                            loaded = super.loadClass(name, false);
                        }
                    }
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        private static boolean isParentFirstClass(String name) {
            return name.startsWith("java.")
                    || name.startsWith("javax.")
                    || name.startsWith("jakarta.")
                    || name.startsWith("sun.")
                    || name.startsWith("com.sun.")
                    || name.startsWith("org.apache.flink.")
                    || name.startsWith("org.slf4j.");
        }
    }
}

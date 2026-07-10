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

import org.apache.flink.agents.api.memory.LongTermMemoryOptions;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.JavaFunction;
import org.apache.flink.agents.plan.PythonFunction;
import org.apache.flink.agents.plan.resourceprovider.PythonResourceProvider;
import org.apache.flink.agents.runtime.PythonMCPResourceDiscovery;
import org.apache.flink.agents.runtime.ResourceCache;
import org.apache.flink.agents.runtime.env.EmbeddedPythonEnvironment;
import org.apache.flink.agents.runtime.env.PythonEnvironmentManager;
import org.apache.flink.agents.runtime.memory.Mem0LongTermMemory;
import org.apache.flink.agents.runtime.metrics.FlinkAgentsMetricGroupImpl;
import org.apache.flink.agents.runtime.python.context.PythonRunnerContextImpl;
import org.apache.flink.agents.runtime.python.utils.JavaResourceAdapter;
import org.apache.flink.agents.runtime.python.utils.PythonActionExecutor;
import org.apache.flink.agents.runtime.python.utils.PythonResourceAdapterImpl;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;
import pemja.core.object.PyObject;

import javax.annotation.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.agents.plan.actions.Utils.requiredVersions;
import static org.apache.flink.agents.plan.actions.Utils.supportAsync;

/**
 * Owns the embedded Python runtime used by {@link ActionExecutionOperator} when an agent plan
 * contains Python actions or Python-defined resources.
 *
 * <p>Owned state:
 *
 * <ul>
 *   <li>The {@link PythonEnvironmentManager} that prepares dependencies and the Pemja runtime.
 *   <li>The {@link PythonInterpreter} obtained from that environment.
 *   <li>The {@link PythonActionExecutor} (when the plan contains Python actions).
 *   <li>The {@link PythonRunnerContextImpl} consumed by Python actions.
 *   <li>The Java/Python resource adapters that bridge resource lookups across languages.
 * </ul>
 *
 * <p>Lifecycle: instantiated by the operator's {@code open()} (lazy — not in the operator
 * constructor), then immediately initialized via {@link #open} in the same call. {@link #open} is a
 * no-op when the agent plan contains no Python actions and no Python resources — in that case all
 * accessors return {@code null} and {@link #isInitialized()} returns {@code false}. {@link
 * #close()} closes the owned resources in the reverse order of creation: {@code
 * pythonActionExecutor} → {@code pythonInterpreter} → {@code pythonEnvironmentManager}.
 *
 * <p>Design constraint: package-private; no manager-to-manager held references. Other managers
 * receive what they need (e.g. the Python runner context, the action executor) via method
 * parameters.
 */
class PythonBridgeManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PythonBridgeManager.class);

    static final String PYTHON_ARTIFACT_PATH_CONFIG = "dynamic-plan.python-artifact.path";
    static final String PYTHON_ARTIFACT_SHA256_CONFIG = "dynamic-plan.python-artifact.sha256";
    static final String PYTHON_RUNTIME_PATH_CONFIG = "dynamic-plan.python-runtime.path";

    private PythonEnvironmentManager pythonEnvironmentManager;
    private final Map<String, PlanPythonRuntime> planRuntimes;
    private String initialPlanId;

    private ExecutionConfig executionConfig;
    private DistributedCache distributedCache;
    private String[] tmpDirs;
    private JobID jobId;
    private FlinkAgentsMetricGroupImpl metricGroup;
    private Runnable mailboxThreadChecker;
    private String jobIdentifier;
    private ClassLoader userCodeClassLoader;

    private boolean initialized;

    PythonBridgeManager() {
        this.planRuntimes = new HashMap<>();
        this.initialized = false;
    }

    /**
     * Initializes the Python runtime if the agent plan needs it.
     *
     * <p>Scans the agent plan for any {@link PythonFunction} action or {@link
     * PythonResourceProvider}. If neither is present, this method is a no-op and {@link
     * #isInitialized()} stays {@code false}. Otherwise it builds the {@link
     * PythonEnvironmentManager}, opens an embedded {@link PythonInterpreter}, constructs the shared
     * {@link PythonRunnerContextImpl}, wires the Java/Python resource adapters, and conditionally
     * initializes the Python action executor and the Python resource adapter (each only when the
     * corresponding component is present in the plan).
     *
     * @param agentPlan the agent plan describing actions and resources.
     * @param resourceCache the resource cache visible to both languages.
     * @param executionConfig used to derive Python dependency information.
     * @param distributedCache used to resolve distributed Python files.
     * @param tmpDirs Flink-managed temp directories made available to Python.
     * @param jobId the Flink job id.
     * @param metricGroup the agent metric group, exposed to Python via the runner context.
     * @param mailboxThreadChecker hook used by the runner context to assert mailbox-thread access.
     * @param jobIdentifier the job identifier used to scope Python state.
     * @param userCodeClassLoader the operator's user-code class loader, propagated to {@link
     *     JavaResourceAdapter} so reflective Java tool resolution sees user jars added via {@code
     *     env.add_jars(...)}.
     */
    void open(
            String initialPlanId,
            AgentPlan agentPlan,
            ResourceCache resourceCache,
            ExecutionConfig executionConfig,
            DistributedCache distributedCache,
            String[] tmpDirs,
            JobID jobId,
            FlinkAgentsMetricGroupImpl metricGroup,
            Runnable mailboxThreadChecker,
            String jobIdentifier,
            ClassLoader userCodeClassLoader)
            throws Exception {
        this.executionConfig = executionConfig;
        this.distributedCache = distributedCache;
        this.tmpDirs = tmpDirs;
        this.jobId = jobId;
        this.metricGroup = metricGroup;
        this.mailboxThreadChecker = mailboxThreadChecker;
        this.jobIdentifier = jobIdentifier;
        this.userCodeClassLoader = userCodeClassLoader;
        this.initialPlanId = initialPlanId;

        getOrCreatePlanRuntime(initialPlanId, agentPlan, resourceCache);
    }

    PlanPythonRuntime getOrCreatePlanRuntime(
            String planId, AgentPlan agentPlan, ResourceCache resourceCache) throws Exception {
        PlanPythonRuntime runtime = planRuntimes.get(planId);
        if (runtime != null) {
            return runtime;
        }

        boolean containPythonAction = containsPythonAction(agentPlan);
        boolean containPythonResource = containsPythonResource(agentPlan);
        boolean mem0Configured = isMem0Configured(agentPlan);

        if (!containPythonAction && !containPythonResource && !mem0Configured) {
            return null;
        }

        PythonInterpreter interpreter = createPythonInterpreter(agentPlan);

        PythonRunnerContextImpl pythonRunnerContext =
                new PythonRunnerContextImpl(
                        metricGroup, mailboxThreadChecker, agentPlan, resourceCache, jobIdentifier);
        JavaResourceAdapter javaResourceAdapter =
                new JavaResourceAdapter(
                        resourceCache.getResourceContext(), interpreter, userCodeClassLoader);

        PythonResourceAdapterImpl pythonResourceAdapter = null;
        if (containPythonResource || mem0Configured) {
            pythonResourceAdapter =
                    initPythonResourceAdapter(
                            agentPlan, resourceCache, javaResourceAdapter, interpreter);
        }

        PythonActionExecutor pythonActionExecutor = null;
        if (containPythonAction || mem0Configured) {
            pythonActionExecutor =
                    initPythonActionExecutor(
                            planId,
                            agentPlan,
                            javaResourceAdapter,
                            pythonRunnerContext,
                            interpreter);
        }

        Mem0LongTermMemory longTermMemory = null;
        if (mem0Configured) {
            longTermMemory =
                    wireLongTermMemory(pythonActionExecutor, pythonResourceAdapter, interpreter);
        }

        runtime =
                new PlanPythonRuntime(
                        pythonActionExecutor,
                        pythonRunnerContext,
                        pythonResourceAdapter,
                        javaResourceAdapter,
                        longTermMemory,
                        interpreter);
        planRuntimes.put(planId, runtime);
        return runtime;
    }

    private PythonInterpreter createPythonInterpreter(AgentPlan agentPlan) throws Exception {
        ensurePythonEnvironment();
        EmbeddedPythonEnvironment env = pythonEnvironmentManager.createEnvironment();
        PythonInterpreterConfig baseConfig = env.getConfig();
        PythonInterpreterConfig.PythonInterpreterConfigBuilder builder =
                PythonInterpreterConfig.newBuilder().setExcType(baseConfig.getExecType());

        String runtimePath = agentPlan.getConfig().getStr(PYTHON_RUNTIME_PATH_CONFIG, null);
        if (runtimePath != null && !runtimePath.trim().isEmpty()) {
            Path path = Path.of(runtimePath);
            org.apache.flink.util.Preconditions.checkState(
                    Files.exists(path), "Python runtime path %s does not exist.", runtimePath);
            builder.addPythonPaths(path.toString());
        }

        String artifactPath = agentPlan.getConfig().getStr(PYTHON_ARTIFACT_PATH_CONFIG, null);
        if (artifactPath != null && !artifactPath.trim().isEmpty()) {
            Path path = Path.of(artifactPath);
            org.apache.flink.util.Preconditions.checkState(
                    Files.exists(path), "Python artifact path %s does not exist.", artifactPath);
            String expectedSha256 =
                    agentPlan.getConfig().getStr(PYTHON_ARTIFACT_SHA256_CONFIG, null);
            if (expectedSha256 != null && !expectedSha256.trim().isEmpty()) {
                String actual =
                        org.apache.flink.agents.runtime.operator.coordinator.PlanIds
                                .sha256HexOfFile(path);
                org.apache.flink.util.Preconditions.checkState(
                        actual.equalsIgnoreCase(expectedSha256.trim()),
                        "Python artifact %s failed sha256 verification: expected %s but was %s.",
                        artifactPath,
                        expectedSha256,
                        actual);
            }
            builder.addPythonPaths(path.toString());
        }

        builder.addPythonPaths(baseConfig.getPaths());
        if (baseConfig.getPythonHome() != null) {
            builder.setPythonHome(baseConfig.getPythonHome());
        }
        if (baseConfig.getWorkingDirectory() != null) {
            builder.setWorkingDirectory(baseConfig.getWorkingDirectory());
        }
        if (baseConfig.getPythonExec() != null) {
            builder.setPythonExec(baseConfig.getPythonExec());
        }
        return new PythonInterpreter(builder.build());
    }

    private void ensurePythonEnvironment() throws Exception {
        if (initialized) {
            return;
        }
        LOG.debug("Begin initialize PythonEnvironmentManager.");
        PythonDependencyInfo dependencyInfo =
                PythonDependencyInfo.create(executionConfig.toConfiguration(), distributedCache);
        pythonEnvironmentManager =
                new PythonEnvironmentManager(
                        dependencyInfo, tmpDirs, new HashMap<>(System.getenv()), jobId);
        pythonEnvironmentManager.open();
        initialized = true;
    }

    private boolean containsPythonAction(AgentPlan agentPlan) {
        return agentPlan.getActions().values().stream()
                .anyMatch(action -> action.getExec() instanceof PythonFunction);
    }

    private boolean containsPythonResource(AgentPlan agentPlan) {
        return agentPlan.getResourceProviders().values().stream()
                .anyMatch(
                        resourceProviderMap ->
                                resourceProviderMap.values().stream()
                                        .anyMatch(
                                                resourceProvider ->
                                                        resourceProvider
                                                                instanceof PythonResourceProvider));
    }

    /**
     * Whether the agent's configuration enables Mem0 as the long-term memory backend.
     *
     * <p>Mirrors the Python {@code _init_long_term_memory}: Mem0 is considered configured only when
     * all three resource references are present (chat model setup, embedding model setup, vector
     * store). Otherwise the Python side returns no LTM and the embedded Python interpreter cost
     * should be avoided. When configured alongside Java actions, the runtime requires a Flink
     * version that ships the async-friendly pemja fix.
     *
     * @param agentPlan the agent plan whose configuration is inspected.
     * @return {@code true} when Mem0 is fully configured.
     */
    private boolean isMem0Configured(AgentPlan agentPlan) {
        var config = agentPlan.getConfig();
        boolean configured =
                config.get(LongTermMemoryOptions.Mem0.CHAT_MODEL_SETUP) != null
                        && config.get(LongTermMemoryOptions.Mem0.EMBEDDING_MODEL_SETUP) != null
                        && config.get(LongTermMemoryOptions.Mem0.VECTOR_STORE) != null;

        boolean containJavaAction =
                agentPlan.getActions().values().stream()
                        .anyMatch(action -> action.getExec() instanceof JavaFunction);

        // Mem0 will call chat model and embedding model in its own thread executor, this behavior
        // is same as the async execution for cross-language resources, and also requires the fix
        // in pemja.
        if (configured && containJavaAction && !supportAsync()) {
            throw new RuntimeException(
                    String.format(
                            "Using Mem0 based Long-Term Memory in java requires flink version higher "
                                    + "than %s. You can upgrade flink or use python api.",
                            requiredVersions));
        }
        return configured;
    }

    /**
     * Pull the {@code long_term_memory} attribute off the Python {@code FlinkRunnerContext} (which
     * {@code create_flink_runner_context} already initialised via {@code _init_long_term_memory})
     * and wrap it as a Java {@link Mem0LongTermMemory}.
     */
    private Mem0LongTermMemory wireLongTermMemory(
            PythonActionExecutor pythonActionExecutor,
            PythonResourceAdapterImpl pythonResourceAdapter,
            PythonInterpreter interpreter) {
        PyObject pyCtx = pythonActionExecutor.getPythonRunnerContext();
        Object pyLtm = interpreter.invokeMethod("python_java_utils", "get_long_term_memory", pyCtx);
        if (pyLtm == null) {
            throw new IllegalStateException(
                    String.format(
                            "Mem0 long-term memory is configured on the Java side but the Python "
                                    + "runner context returned no long-term memory. Verify that %s, "
                                    + "%s, and %s all reference resources that exist and that the "
                                    + "Python-side _init_long_term_memory succeeded.",
                            LongTermMemoryOptions.Mem0.CHAT_MODEL_SETUP.getKey(),
                            LongTermMemoryOptions.Mem0.EMBEDDING_MODEL_SETUP.getKey(),
                            LongTermMemoryOptions.Mem0.VECTOR_STORE.getKey()));
        }
        return new Mem0LongTermMemory(pythonResourceAdapter, (PyObject) pyLtm);
    }

    private PythonActionExecutor initPythonActionExecutor(
            String planId,
            AgentPlan agentPlan,
            JavaResourceAdapter javaResourceAdapter,
            PythonRunnerContextImpl pythonRunnerContext,
            PythonInterpreter interpreter)
            throws Exception {
        PythonActionExecutor pythonActionExecutor =
                new PythonActionExecutor(
                        interpreter,
                        agentPlan,
                        javaResourceAdapter,
                        pythonRunnerContext,
                        jobIdentifier,
                        pythonFunctionScope(planId, interpreter));
        pythonActionExecutor.open();
        return pythonActionExecutor;
    }

    /**
     * Framework-level function-cache scope: a runtime-instance identity (job + plan + interpreter
     * identity), never checkpointed. Multiple interpreters do not isolate module-global caches in
     * embedded Python, so the cache key must carry this scope.
     */
    private String pythonFunctionScope(String planId, PythonInterpreter interpreter) {
        return jobIdentifier
                + "-plan-"
                + planId
                + "-interpreter-"
                + System.identityHashCode(interpreter);
    }

    private PythonResourceAdapterImpl initPythonResourceAdapter(
            AgentPlan agentPlan,
            ResourceCache resourceCache,
            JavaResourceAdapter javaResourceAdapter,
            PythonInterpreter interpreter)
            throws Exception {
        PythonResourceAdapterImpl pythonResourceAdapter =
                new PythonResourceAdapterImpl(
                        resourceCache.getResourceContext(), interpreter, javaResourceAdapter);
        pythonResourceAdapter.open();
        PythonMCPResourceDiscovery.discoverPythonMCPResources(
                agentPlan.getResourceProviders(), pythonResourceAdapter, resourceCache);
        return pythonResourceAdapter;
    }

    /**
     * @return the Python action executor for the initial plan, or {@code null} if the agent plan
     *     contains no Python actions (or {@link #open} has not yet been called).
     */
    @Nullable
    PythonActionExecutor getPythonActionExecutor() {
        return getPythonActionExecutor(initialPlanId);
    }

    @Nullable
    PythonActionExecutor getPythonActionExecutor(String planId) {
        PlanPythonRuntime runtime = planRuntimes.get(planId);
        return runtime == null ? null : runtime.pythonActionExecutor;
    }

    /**
     * @return the Python runner context for the initial plan, or {@code null} if no Python runtime
     *     was initialized because the agent plan has neither Python actions nor Python resources.
     */
    @Nullable
    PythonRunnerContextImpl getPythonRunnerContext() {
        return getPythonRunnerContext(initialPlanId);
    }

    @Nullable
    PythonRunnerContextImpl getPythonRunnerContext(String planId) {
        PlanPythonRuntime runtime = planRuntimes.get(planId);
        return runtime == null ? null : runtime.pythonRunnerContext;
    }

    /**
     * @return the wired Mem0-backed long-term memory, or {@code null} when Mem0 is not configured.
     */
    @Nullable
    Mem0LongTermMemory getLongTermMemory() {
        return getLongTermMemory(initialPlanId);
    }

    @Nullable
    Mem0LongTermMemory getLongTermMemory(String planId) {
        PlanPythonRuntime runtime = planRuntimes.get(planId);
        return runtime == null ? null : runtime.longTermMemory;
    }

    boolean isInitialized() {
        return initialized;
    }

    void releasePlan(String planId) throws Exception {
        PlanPythonRuntime runtime = planRuntimes.remove(planId);
        if (runtime != null) {
            runtime.close();
        }
    }

    @Override
    public void close() throws Exception {
        Exception firstException = null;
        for (PlanPythonRuntime runtime : planRuntimes.values()) {
            try {
                runtime.close();
            } catch (Exception e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        planRuntimes.clear();
        if (pythonEnvironmentManager != null) {
            try {
                pythonEnvironmentManager.close();
            } catch (Exception e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    static final class PlanPythonRuntime implements AutoCloseable {
        private final PythonActionExecutor pythonActionExecutor;
        private final PythonRunnerContextImpl pythonRunnerContext;
        private final PythonResourceAdapterImpl pythonResourceAdapter;
        private final JavaResourceAdapter javaResourceAdapter;
        private final Mem0LongTermMemory longTermMemory;
        private final PythonInterpreter pythonInterpreter;

        private PlanPythonRuntime(
                PythonActionExecutor pythonActionExecutor,
                PythonRunnerContextImpl pythonRunnerContext,
                PythonResourceAdapterImpl pythonResourceAdapter,
                JavaResourceAdapter javaResourceAdapter,
                Mem0LongTermMemory longTermMemory,
                PythonInterpreter pythonInterpreter) {
            this.pythonActionExecutor = pythonActionExecutor;
            this.pythonRunnerContext = pythonRunnerContext;
            this.pythonResourceAdapter = pythonResourceAdapter;
            this.javaResourceAdapter = javaResourceAdapter;
            this.longTermMemory = longTermMemory;
            this.pythonInterpreter = pythonInterpreter;
        }

        @Override
        public void close() throws Exception {
            Exception firstException = null;
            if (pythonActionExecutor != null) {
                try {
                    pythonActionExecutor.close();
                } catch (Exception e) {
                    firstException = e;
                }
            }
            if (pythonInterpreter != null) {
                try {
                    pythonInterpreter.close();
                } catch (Exception e) {
                    if (firstException == null) {
                        firstException = e;
                    } else {
                        firstException.addSuppressed(e);
                    }
                }
            }
            if (firstException != null) {
                throw firstException;
            }
        }
    }
}

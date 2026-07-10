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
import org.apache.flink.agents.runtime.CompileUtils;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateRequest;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves a coordination request delivered over the JM REST endpoint switches the plan of a running
 * MiniCluster job at the next checkpoint barrier, via both control paths:
 *
 * <ul>
 *   <li>{@code pythonRestSwitchesPlan} — a pure-Python client POSTs the request (no JVM).
 *   <li>{@code javaClientRestSwitchesPlan} — {@link RestClusterClient#sendCoordinationRequest}.
 * </ul>
 *
 * plus {@code pythonRequestBytesMatchJava}, which checks the pure-Python request bytes are
 * byte-for-byte identical to Java's serialization.
 */
public class CoordinatedPlanUpdateRestE2ETest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static final CopyOnWriteArrayList<String> RESULTS = new CopyOnWriteArrayList<>();

    @Test
    void pythonRequestBytesMatchJava() throws Exception {
        // The Python-specific crux: a pure-Python client (no JVM) hand-builds the Java-serialized
        // PlanUpdateRequest; its bytes must be byte-for-byte what Java produces, i.e. exactly the
        // payload the REST endpoint accepts. No cluster needed for this check.
        String autoTriageJson = OBJECT_MAPPER.writeValueAsString(createPlan("autoTriageAction"));
        byte[] javaBytes =
                org.apache.flink.util.InstantiationUtil.serializeObject(
                        new PlanUpdateRequest(autoTriageJson));
        byte[] pythonBytes = pythonSerializedRequest(autoTriageJson);
        assertThat(pythonBytes)
                .as("python-built PlanUpdateRequest bytes equal Java serialization")
                .isEqualTo(javaBytes);
    }

    @Test
    void pythonRestSwitchesPlan() throws Exception {
        // The pure-Python client POSTs to the real REST coordination endpoint and switches the
        // plan — no JVM, no id computation, no lookup on the client side.
        runSwitchScenario(
                (rest, jobId, autoTriageJson) -> {
                    String restBase = "http://" + rest.getHost() + ":" + rest.getPort();
                    int exit = pythonSwitchPlan(restBase, jobId.toString(), autoTriageJson);
                    assertThat(exit).as("python REST plan switch exit code").isEqualTo(0);
                });
    }

    @Test
    void javaClientRestSwitchesPlan() throws Exception {
        // The Java client path: RestClusterClient delivers the same coordination request over the
        // REST endpoint, addressing the coordinator by the operator uid (hashed server-side).
        runSwitchScenario(
                (rest, jobId, autoTriageJson) -> {
                    Configuration clientConf = new Configuration();
                    clientConf.set(RestOptions.ADDRESS, rest.getHost());
                    clientConf.set(RestOptions.PORT, rest.getPort());
                    try (RestClusterClient<String> client =
                            new RestClusterClient<>(clientConf, "minicluster")) {
                        client.sendCoordinationRequest(
                                        jobId,
                                        CompileUtils.COORDINATED_OPERATOR_UID,
                                        new PlanUpdateRequest(autoTriageJson))
                                .get();
                    }
                });
    }

    /**
     * Runs the two-ticket job and applies {@code switcher} between ticket 1001 (routed by v1) and
     * ticket 1002 (which must route by v2), then asserts the routing switched.
     */
    private void runSwitchScenario(PlanSwitcher switcher) throws Exception {
        RESULTS.clear();
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "0");
        MiniClusterConfiguration mcc =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(2)
                        .setConfiguration(conf)
                        .build();
        try (MiniCluster cluster = new MiniCluster(mcc)) {
            cluster.start();
            URI rest = cluster.getRestAddress().get();

            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(conf);
            env.setParallelism(1);
            env.disableOperatorChaining();
            // The plan switch is checkpoint-aligned: it becomes effective at the first barrier
            // after the update arrives, so the job must be checkpointing.
            env.enableCheckpointing(500L);
            KeyedStream<Long, Long> tickets = env.addSource(new TwoTicketSource()).keyBy(t -> t);
            CompileUtils.connectToAgentWithCoordinator(tickets, createPlan("manualReviewAction"))
                    .addSink(new CollectSink());

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            cluster.submitJob(jobGraph).get();
            JobID jobId = jobGraph.getJobID();

            // Wait until ticket 1001 has been handled by v1, so the coordinator is live.
            long deadline = System.currentTimeMillis() + 30_000L;
            while (RESULTS.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(100L);
            }

            String autoTriageJson =
                    OBJECT_MAPPER.writeValueAsString(createPlan("autoTriageAction"));
            // The coordinator rejects updates while a checkpoint is in flight (single-point
            // switch decision); with a 500ms checkpoint interval a submission can collide, so
            // retry — which is also the documented client-side usage.
            long submitDeadline = System.currentTimeMillis() + 30_000L;
            while (true) {
                try {
                    switcher.switchPlan(rest, jobId, autoTriageJson);
                    break;
                } catch (Exception | AssertionError t) {
                    if (System.currentTimeMillis() >= submitDeadline) {
                        throw t;
                    }
                    Thread.sleep(200L);
                }
            }

            long deadline2 = System.currentTimeMillis() + 30_000L;
            while (RESULTS.size() < 2 && System.currentTimeMillis() < deadline2) {
                Thread.sleep(200L);
            }
        }
        assertThat(RESULTS).containsExactly("manual-review:1001", "auto-triage:1002");
    }

    /** Applies a plan switch to the running job via some delivery channel. */
    @FunctionalInterface
    private interface PlanSwitcher {
        void switchPlan(URI rest, JobID jobId, String autoTriageJson) throws Exception;
    }

    /** Runs the pure-Python serializer and returns the request bytes it produced. */
    private static byte[] pythonSerializedRequest(String planJson) throws Exception {
        java.nio.file.Path script =
                java.nio.file.Paths.get("")
                        .toAbsolutePath()
                        .getParent()
                        .resolve("e2e-test/test-scripts/send_plan_via_coordinator.py");
        org.assertj.core.api.Assumptions.assumeThat(java.nio.file.Files.exists(script))
                .as("python sender script present")
                .isTrue();
        java.nio.file.Path planFile = java.nio.file.Files.createTempFile("plan", ".json");
        java.nio.file.Files.writeString(planFile, planJson);
        java.nio.file.Path outFile = java.nio.file.Files.createTempFile("req", ".bin");
        ProcessBuilder pb =
                new ProcessBuilder(
                        "python3",
                        script.toString(),
                        "--emit-bytes",
                        planFile.toString(),
                        outFile.toString());
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        if (!p.waitFor(60, java.util.concurrent.TimeUnit.SECONDS) || p.exitValue() != 0) {
            p.destroyForcibly();
            throw new IllegalStateException("python serializer failed");
        }
        return java.nio.file.Files.readAllBytes(outFile);
    }

    /** Runs the pure-Python sender to POST the plan switch over REST; returns its exit code. */
    private static int pythonSwitchPlan(String restBase, String jobId, String planJson)
            throws Exception {
        java.nio.file.Path script =
                java.nio.file.Paths.get("")
                        .toAbsolutePath()
                        .getParent()
                        .resolve("e2e-test/test-scripts/send_plan_via_coordinator.py");
        java.nio.file.Path planFile = java.nio.file.Files.createTempFile("plan", ".json");
        java.nio.file.Files.writeString(planFile, planJson);
        ProcessBuilder pb =
                new ProcessBuilder(
                        "python3", script.toString(), restBase, jobId, planFile.toString());
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(60, java.util.concurrent.TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("python REST sender timed out");
        }
        if (p.exitValue() != 0) {
            System.out.println("python sender output:\n" + out);
        }
        return p.exitValue();
    }

    public static void manualReviewAction(Event event, RunnerContext context) {
        context.sendEvent(
                new OutputEvent("manual-review:" + InputEvent.fromEvent(event).getInput()));
    }

    public static void autoTriageAction(Event event, RunnerContext context) {
        context.sendEvent(new OutputEvent("auto-triage:" + InputEvent.fromEvent(event).getInput()));
    }

    private static AgentPlan createPlan(String method) throws Exception {
        Action action =
                new Action(
                        method,
                        new JavaFunction(
                                CoordinatedPlanUpdateRestE2ETest.class,
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

    private static final class TwoTicketSource implements SourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(1001L);
            }
            Thread.sleep(8_000L);
            if (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(1002L);
                }
            }
            while (running) {
                Thread.sleep(200L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class CollectSink implements SinkFunction<Object> {
        @Override
        public void invoke(Object value, Context context) {
            RESULTS.add(value.toString());
        }
    }
}

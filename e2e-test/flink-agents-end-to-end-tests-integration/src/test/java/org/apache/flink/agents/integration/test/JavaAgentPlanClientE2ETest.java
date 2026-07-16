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
package org.apache.flink.agents.integration.test;

import org.apache.flink.agents.api.AgentsExecutionEnvironment;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.EventType;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.agents.Agent;
import org.apache.flink.agents.api.annotation.Action;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.runtime.client.AgentPlanClient;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies Java client plan activation and name-based routing on a running Flink job. */
public class JavaAgentPlanClientE2ETest {

    private static final String TARGET_AGENT = "target-agent";
    private static final String CONTROL_AGENT = "control-agent";
    private static final Set<String> RESULTS = ConcurrentHashMap.newKeySet();

    @Test
    void updatesOnlyTheNamedAgentAtTheSecondCheckpoint(@TempDir Path tempDir) throws Exception {
        RESULTS.clear();
        Path inputDir = Files.createDirectory(tempDir.resolve("input"));
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT, "0");
        MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(3)
                        .setNumSlotsPerTaskManager(1)
                        .setConfiguration(configuration)
                        .build();

        try (MiniCluster cluster = new MiniCluster(miniClusterConfiguration)) {
            cluster.start();
            URI restAddress = cluster.getRestAddress().get();
            JobGraph jobGraph = createJobGraph(configuration, inputDir);
            cluster.submitJob(jobGraph).get();
            JobID jobId = jobGraph.getJobID();
            waitUntilRunning(cluster, jobId);

            addInputAndWaitFor(inputDir, 1L, "control-v1:target-v1:1");

            try (AgentPlanClient client =
                    new AgentPlanClient(restAddress.getHost(), restAddress.getPort())) {
                AgentPlanClient.UpdateResult staged =
                        client.updateAgentPlan(
                                jobId, TARGET_AGENT, new AgentPlan(new TargetV2Agent()));

                assertEquals(1L, staged.getPlanVersion());
                assertEquals(1, staged.getRegisteredSubtasks());

                addInputAndWaitFor(inputDir, 2L, "control-v1:target-v1:2");

                triggerCheckpoint(cluster, jobId);
                addInputAndWaitFor(inputDir, 3L, "control-v1:target-v1:3");

                AgentPlanClient.UpdateResult staged2 =
                        client.updateAgentPlan(
                                jobId, CONTROL_AGENT, new AgentPlan(new ControlV2Agent()));

                assertEquals(1L, staged2.getPlanVersion());
                assertEquals(1, staged2.getRegisteredSubtasks());

                triggerCheckpoint(cluster, jobId);
                addInputAndWaitFor(inputDir, 4L, "control-v1:target-v2:4");

                addInputAndWaitFor(inputDir, 5L, "control-v1:target-v2:5");

                triggerCheckpoint(cluster, jobId);
                addInputAndWaitFor(inputDir, 6L, "control-v2:target-v2:6");

                triggerCheckpoint(cluster, jobId);
                addInputAndWaitFor(inputDir, 7L, "control-v2:target-v2:7");
            }

            assertEquals(JobStatus.RUNNING, cluster.getJobStatus(jobId).get(30, TimeUnit.SECONDS));
            cluster.cancelJob(jobId).get(30, TimeUnit.SECONDS);
        }
    }

    private static JobGraph createJobGraph(Configuration configuration, Path inputDir) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(Duration.ofHours(1).toMillis());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new org.apache.flink.core.fs.Path(inputDir.toUri()))
                        .monitorContinuously(Duration.ofMillis(50))
                        .build();
        org.apache.flink.streaming.api.datastream.DataStream<Long> input =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "gated-input")
                        .map(Long::valueOf);
        AgentsExecutionEnvironment agents = AgentsExecutionEnvironment.getExecutionEnvironment(env);
        DataStream<Object> output1 =
                agents.fromDataStream(input, value -> value)
                        .apply(TARGET_AGENT, new TargetV1Agent())
                        .toDataStream();

        DataStream<String> map = output1.map(Object::toString);

        agents.fromDataStream(map, value -> value)
                .apply(CONTROL_AGENT, new ControlV1Agent())
                .toDataStream()
                .map(new CollectMap())
                .sinkTo(new DiscardingSink<>());
        return env.getStreamGraph().getJobGraph();
    }

    private static void addInputAndWaitFor(Path inputDir, long value, String... expected)
            throws Exception {
        Path temporary = inputDir.resolve("." + value + ".tmp");
        Files.writeString(temporary, Long.toString(value));
        Path input = inputDir.resolve(value + ".txt");
        try {
            Files.move(temporary, input, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(temporary, input);
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!RESULTS.containsAll(Set.of(expected)) && System.nanoTime() < deadline) {
            Thread.sleep(50L);
        }
        for (String result : expected) {
            assertTrue(
                    RESULTS.contains(result), "Missing result " + result + "; actual=" + RESULTS);
        }
    }

    private static void waitUntilRunning(MiniCluster cluster, JobID jobId) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        JobStatus status = cluster.getJobStatus(jobId).get();
        while (status != JobStatus.RUNNING && System.nanoTime() < deadline) {
            Thread.sleep(50L);
            status = cluster.getJobStatus(jobId).get();
        }
        assertEquals(JobStatus.RUNNING, status);
    }

    private static void triggerCheckpoint(MiniCluster cluster, JobID jobId) throws Exception {
        cluster.triggerCheckpoint(jobId, CheckpointType.CONFIGURED).get(30, TimeUnit.SECONDS);
    }

    public static final class TargetV1Agent extends Agent {
        @Action(EventType.InputEvent)
        public static void onInput(Event event, RunnerContext context) {
            emit("target-v1", event, context);
        }
    }

    public static final class TargetV2Agent extends Agent {
        @Action(EventType.InputEvent)
        public static void onInput(Event event, RunnerContext context) {
            emit("target-v2", event, context);
        }
    }

    public static final class ControlV1Agent extends Agent {
        @Action(EventType.InputEvent)
        public static void onInput(Event event, RunnerContext context) {
            emit("control-v1", event, context);
        }
    }

    public static final class ControlV2Agent extends Agent {
        @Action(EventType.InputEvent)
        public static void onInput(Event event, RunnerContext context) {
            emit("control-v2", event, context);
        }
    }

    private static void emit(String prefix, Event event, RunnerContext context) {
        context.sendEvent(new OutputEvent(prefix + ":" + InputEvent.fromEvent(event).getInput()));
    }

    private static final class CollectMap implements MapFunction<Object, Object> {
        @Override
        public Object map(Object value) {
            RESULTS.add(value.toString());
            return value;
        }
    }
}

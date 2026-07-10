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

import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.plan.AgentConfiguration;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.JavaFunction;
import org.apache.flink.agents.plan.actions.Action;
import org.apache.flink.agents.runtime.CompileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end: a dynamic-plan job (dynamic-plan.enabled) built through {@link CompileUtils} submits
 * to a real MiniCluster, attaches exactly one OperatorCoordinator to the agent vertex, runs, and
 * produces v1 output. The push round-trip itself is unit-covered by {@code
 * CoordinatedActionExecutionOperatorTest}.
 */
public class CoordinatedPlanUpdateMiniClusterE2ETest {

    static final CopyOnWriteArrayList<String> RESULTS = new CopyOnWriteArrayList<>();

    @Test
    void coordinatorPublishesNewPlanWhileRunning() throws Exception {
        RESULTS.clear();
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "0"); // avoid clashing with a local 8081
        MiniClusterConfiguration mcc =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(1)
                        .setConfiguration(conf)
                        .build();
        try (MiniCluster cluster = new MiniCluster(mcc)) {
            cluster.start();

            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(conf);
            env.setParallelism(1);
            env.disableOperatorChaining(); // keep the coordinated operator its own vertex
            KeyedStream<Long, Long> tickets = env.addSource(new TwoTicketSource()).keyBy(t -> t);
            CompileUtils.connectToAgentWithCoordinator(tickets, createPlan("manualReviewAction"))
                    .addSink(new CollectSink());

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            // dynamic-plan wiring must attach exactly one OperatorCoordinator to the agent
            // operator vertex (and nowhere else).
            long coordinatedVertices =
                    java.util.stream.StreamSupport.stream(
                                    jobGraph.getVertices().spliterator(), false)
                            .filter(v -> !v.getOperatorCoordinators().isEmpty())
                            .peek(
                                    v ->
                                            assertThat(v.getName())
                                                    .contains(
                                                            "coordinated-action-execute-operator"))
                            .count();
            assertThat(coordinatedVertices).isEqualTo(1L);

            cluster.submitJob(jobGraph).get();
            var jobId = jobGraph.getJobID();

            // Job runs with v1 and produces output; coordinator is live for plan pushes.
            long deadline = System.currentTimeMillis() + 30_000L;
            while (RESULTS.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(100L);
            }
            assertThat(cluster.getJobStatus(jobId).get().toString()).isEqualTo("RUNNING");
        }
        assertThat(RESULTS).contains("manual-review:1001");
    }

    public static void manualReviewAction(Event event, RunnerContext context) {
        context.sendEvent(
                new OutputEvent("manual-review:" + InputEvent.fromEvent(event).getInput()));
    }

    private static AgentPlan createPlan(String method) throws Exception {
        Action action =
                new Action(
                        method,
                        new JavaFunction(
                                CoordinatedPlanUpdateMiniClusterE2ETest.class,
                                method,
                                new Class<?>[] {Event.class, RunnerContext.class}),
                        Collections.singletonList(InputEvent.EVENT_TYPE));
        Map<String, Action> actions = new HashMap<>();
        actions.put(action.getName(), action);
        Map<String, List<Action>> byEvent = new HashMap<>();
        byEvent.put(InputEvent.EVENT_TYPE, Collections.singletonList(action));
        return new AgentPlan(actions, byEvent, new HashMap<>(), new AgentConfiguration());
    }

    private static final class TwoTicketSource implements SourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(1001L);
            }
            Thread.sleep(5_000L);
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

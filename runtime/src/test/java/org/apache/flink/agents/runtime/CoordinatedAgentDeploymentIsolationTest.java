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

package org.apache.flink.agents.runtime;

import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.runtime.operator.ActionExecutionOperatorTest;
import org.apache.flink.agents.runtime.operator.CoordinatedActionExecutionOperatorFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the deployment isolation contract of the coordinated agent operator. */
class CoordinatedAgentDeploymentIsolationTest {

    private static final String AGENT_NAME = "review-agent";
    private static final String AGENT_SLOT_SHARING_GROUP =
            "flink-agents-action-execution:" + AGENT_NAME;
    private static final AgentPlan AGENT_PLAN =
            ActionExecutionOperatorTest.TestAgent.getAgentPlan(false);

    @Test
    void coordinatedOperatorMustNotBeChained() {
        CoordinatedActionExecutionOperatorFactory<Long, Object> factory =
                new CoordinatedActionExecutionOperatorFactory<>(AGENT_PLAN, true);

        assertThat(factory.getChainingStrategy()).isEqualTo(ChainingStrategy.NEVER);
    }

    @Test
    void coordinatedOperatorMustUseDedicatedSlotSharingGroup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Long, Long> input = env.fromData(1L).keyBy(value -> value);

        DataStream<Object> output =
                CompileUtils.connectToAgentWithCoordinator(input, AGENT_NAME, AGENT_PLAN);

        assertThat(output.getTransformation().getUid())
                .isEqualTo("flink-agents-coordinated-operator:" + AGENT_NAME);
        assertThat(output.getTransformation().getName())
                .isEqualTo("coordinated-action-execute-operator:" + AGENT_NAME);
        assertThat(output.getTransformation().getSlotSharingGroup())
                .hasValueSatisfying(
                        group -> assertThat(group.getName()).isEqualTo(AGENT_SLOT_SHARING_GROUP));
    }
}

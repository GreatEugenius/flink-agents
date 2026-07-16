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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.runtime.operator.ActionExecutionOperatorFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/** A utility class that bridges Flink DataStream/SQL with the Flink Agents agent. */
public class CompileUtils {

    // ============================ invoke by python ====================================
    public static DataStream<byte[]> connectToAgent(
            KeyedStream<Row, Row> inputDataStream, String agentName, String agentPlanJson)
            throws JsonProcessingException {
        // deserialize agent plan json.
        AgentPlan agentPlan = new ObjectMapper().readValue(agentPlanJson, AgentPlan.class);
        return connectToAgent(
                inputDataStream, agentName, agentPlan, TypeInformation.of(byte[].class), false);
    }

    // ============================ invoke by java ====================================
    public static <IN, K> DataStream<Object> connectToAgent(
            DataStream<IN> inputStream,
            KeySelector<IN, K> keySelector,
            String agentName,
            AgentPlan agentPlan) {
        return connectToAgent(inputStream.keyBy(keySelector), agentName, agentPlan);
    }

    public static <IN, K> DataStream<Object> connectToAgent(
            KeyedStream<IN, K> keyedInputStream, String agentName, AgentPlan agentPlan) {
        return connectToAgent(
                keyedInputStream, agentName, agentPlan, TypeInformation.of(Object.class), true);
    }

    // ============================ basic ====================================
    /**
     * Connects the given KeyedStream to the Flink Agents agent.
     *
     * <p>This method accepts a keyed DataStream and applies the specified agent plan to it. The
     * source of the input stream determines the data format: Java streams provide Objects, while
     * Python streams use serialized byte arrays.
     *
     * @param keyedInputStream The input keyed DataStream.
     * @param agentPlan The agent plan to be executed.
     * @param inputIsJava A flag indicating whether the input stream originates from Java. - If
     *     true, input and output types are Java Objects. - If false, input and output types are
     *     byte[].
     * @param <K> The type of the key used in the keyed DataStream.
     * @param <IN> The type of the input data (Object or byte[]).
     * @param <OUT> The type of the output data (Object or byte[]).
     * @return The processed DataStream as the result of the agent.
     */
    private static <K, IN, OUT> DataStream<OUT> connectToAgent(
            KeyedStream<IN, K> keyedInputStream,
            String agentName,
            AgentPlan agentPlan,
            TypeInformation<OUT> outTypeInformation,
            boolean inputIsJava) {
        validateAgentName(agentName);
        return (DataStream<OUT>)
                keyedInputStream
                        .transform(
                                getOperatorName(agentName),
                                outTypeInformation,
                                new ActionExecutionOperatorFactory(agentPlan, inputIsJava))
                        .uid(getCoordinatedOperatorUid(agentName))
                        .setParallelism(keyedInputStream.getParallelism());
    }

    private static final String COORDINATED_OPERATOR_UID_PREFIX =
            "flink-agents-coordinated-operator:";

    private static final String COORDINATED_OPERATOR_NAME_PREFIX =
            "coordinated-action-execute-operator:";

    public static void validateAgentName(String agentName) {
        Preconditions.checkArgument(
                agentName != null && !agentName.trim().isEmpty(),
                "The agent name must not be blank.");
    }

    public static String getCoordinatedOperatorUid(String agentName) {
        validateAgentName(agentName);
        return COORDINATED_OPERATOR_UID_PREFIX + agentName;
    }

    private static String getOperatorName(String agentName) {
        validateAgentName(agentName);
        return COORDINATED_OPERATOR_NAME_PREFIX + agentName;
    }
}

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

import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.runtime.actionstate.ActionStateStore;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * Factory that wires an {@link ActionExecutionOperator} (with dynamic plan enabled) to a {@link
 * PlanUpdateCoordinator}: it registers the operator as an event handler and provides the
 * coordinator.
 */
public class CoordinatedActionExecutionOperatorFactory<IN, OUT>
        extends AbstractStreamOperatorFactory<OUT>
        implements CoordinatedOperatorFactory<OUT>, OneInputStreamOperatorFactory<IN, OUT> {

    private final AgentPlan agentPlan;
    private final Boolean inputIsJava;
    private final ActionStateStore actionStateStore;

    public CoordinatedActionExecutionOperatorFactory(AgentPlan agentPlan, Boolean inputIsJava) {
        this(agentPlan, inputIsJava, null);
    }

    public CoordinatedActionExecutionOperatorFactory(
            AgentPlan agentPlan, Boolean inputIsJava, ActionStateStore actionStateStore) {
        this.agentPlan = agentPlan;
        this.inputIsJava = inputIsJava;
        this.actionStateStore = actionStateStore;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        ActionExecutionOperator<IN, OUT> op =
                new ActionExecutionOperator<>(
                        agentPlan,
                        inputIsJava,
                        parameters.getProcessingTimeService(),
                        parameters.getMailboxExecutor(),
                        actionStateStore);
        OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        parameters.getOperatorEventDispatcher().registerEventHandler(operatorId, op);
        op.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        return (T) op;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String name, OperatorID operatorId) {
        return new PlanUpdateCoordinator.Provider(operatorId);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return ActionExecutionOperator.class;
    }
}

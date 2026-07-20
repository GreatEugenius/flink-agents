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
package org.apache.flink.agents.runtime.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.runtime.CompileUtils;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateRequest;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateResponse;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ExecutionException;

/** Client for submitting a complete AgentPlan update to a running Flink Agents job. */
public final class AgentPlanClient implements AutoCloseable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String CLUSTER_ID = "flink-agents-plan-client";

    private final RestClusterClient<String> restClient;

    /** Creates a client for a JobManager REST host and port. */
    public AgentPlanClient(String restAddress, int restPort) throws Exception {
        this(new AgentRestClusterClient(createRestConfiguration(restAddress, restPort)));
    }

    /** Reflection entry point used by the Python client. */
    private static AgentPlanClient create(String restAddress, Integer restPort) throws Exception {
        Preconditions.checkNotNull(restPort, "REST port must not be null");
        return new AgentPlanClient(restAddress, restPort);
    }

    @VisibleForTesting
    AgentPlanClient(RestClusterClient<String> restClient) {
        this.restClient = Preconditions.checkNotNull(restClient, "restClient must not be null");
    }

    private static Configuration createRestConfiguration(String restAddress, int restPort) {
        Preconditions.checkArgument(
                restAddress != null && !restAddress.trim().isEmpty(),
                "REST address must not be blank");
        Preconditions.checkArgument(
                restPort > 0 && restPort <= 65535,
                "REST port must be between 1 and 65535: %s",
                restPort);
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.ADDRESS, restAddress.trim());
        configuration.set(RestOptions.PORT, restPort);
        return configuration;
    }

    /** Serializes and submits a complete plan for the named deployed agent. */
    public UpdateResult updateAgentPlan(JobID jobId, String agentName, AgentPlan agentPlan)
            throws Exception {
        Preconditions.checkNotNull(agentPlan, "agentPlan must not be null");
        return updateAgentPlan(jobId, agentName, OBJECT_MAPPER.writeValueAsString(agentPlan));
    }

    /** Submits a complete plan JSON document for the named deployed agent. */
    public UpdateResult updateAgentPlan(JobID jobId, String agentName, String agentPlanJson)
            throws Exception {
        Preconditions.checkNotNull(jobId, "jobId must not be null");
        Preconditions.checkNotNull(agentPlanJson, "agentPlanJson must not be null");
        String operatorUid = CompileUtils.getCoordinatedOperatorUid(agentName);
        CoordinationResponse response;
        try {
            response =
                    CoordinationRequestUtils.send(
                                    restClient,
                                    jobId,
                                    operatorUid,
                                    new PlanUpdateRequest(agentPlanJson))
                            .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
        if (!(response instanceof PlanUpdateResponse)) {
            throw new IllegalStateException(
                    "Expected PlanUpdateResponse but received "
                            + (response == null ? "null" : response.getClass().getName()));
        }
        PlanUpdateResponse planUpdateResponse = (PlanUpdateResponse) response;
        return new UpdateResult(
                planUpdateResponse.planVersion,
                planUpdateResponse.planId,
                planUpdateResponse.subtasksNotified);
    }

    /** String-only bridge used by Python's reflection helper. */
    public UpdateResult updateAgentPlan(String jobId, String agentName, String agentPlanJson)
            throws Exception {
        Preconditions.checkNotNull(jobId, "jobId must not be null");
        return updateAgentPlan(JobID.fromHexString(jobId), agentName, agentPlanJson);
    }

    private static Exception unwrapExecutionException(ExecutionException exception) {
        Throwable cause = exception.getCause();
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new RuntimeException(cause);
    }

    @Override
    public void close() throws Exception {
        restClient.close();
    }

    /**
     * Confirmation that the current coordinator attempt accepted an update as a volatile candidate.
     * It does not mean the update was announced, made durable, or activated.
     */
    public static final class UpdateResult {
        private final long planVersion;
        private final String planId;
        private final int registeredSubtasks;

        private UpdateResult(long planVersion, String planId, int registeredSubtasks) {
            this.planVersion = planVersion;
            this.planId = planId;
            this.registeredSubtasks = registeredSubtasks;
        }

        public long getPlanVersion() {
            return planVersion;
        }

        public String getPlanId() {
            return planId;
        }

        /** Number of subtask gateways registered when the update was accepted. */
        public int getRegisteredSubtasks() {
            return registeredSubtasks;
        }
    }

    /** Ensures coordination responses are deserialized with the Flink Agents classloader. */
    private static final class AgentRestClusterClient extends RestClusterClient<String> {
        private AgentRestClusterClient(Configuration configuration) throws Exception {
            super(configuration, CLUSTER_ID);
        }
    }
}

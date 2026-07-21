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
import org.apache.flink.agents.plan.AgentConfiguration;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.runtime.CompileUtils;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateRequest;
import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateResponse;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AgentPlanClientTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    @Test
    void serializesAndStagesCompleteAgentPlan() throws Exception {
        RestClusterClient<String> restClient = mock(RestClusterClient.class);
        AgentPlanClient client = new AgentPlanClient(restClient);
        JobID jobId = new JobID();
        String agentName = "review-agent";
        PlanUpdateResponse expected = new PlanUpdateResponse(2L, "plan-id", 3);
        when(restClient.sendCoordinationRequest(
                        org.mockito.ArgumentMatchers.eq(jobId),
                        org.mockito.ArgumentMatchers.eq(
                                CompileUtils.getCoordinatedOperatorUid(agentName)),
                        org.mockito.ArgumentMatchers.any(PlanUpdateRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expected));
        AgentPlan plan =
                new AgentPlan(
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new AgentConfiguration());

        AgentPlanClient.UpdateResult actual = client.updateAgentPlan(jobId, agentName, plan);

        assertThat(actual.getPlanVersion()).isEqualTo(2L);
        assertThat(actual.getPlanId()).isEqualTo("plan-id");
        assertThat(actual.getRegisteredSubtasks()).isEqualTo(3);
        ArgumentCaptor<PlanUpdateRequest> requestCaptor =
                ArgumentCaptor.forClass(PlanUpdateRequest.class);
        verify(restClient)
                .sendCoordinationRequest(
                        org.mockito.ArgumentMatchers.eq(jobId),
                        org.mockito.ArgumentMatchers.eq(
                                CompileUtils.getCoordinatedOperatorUid(agentName)),
                        requestCaptor.capture());
        AgentPlan decoded =
                OBJECT_MAPPER.readValue(requestCaptor.getValue().agentPlanJson, AgentPlan.class);
        assertThat(decoded.getActions()).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    void stringBridgeReturnsOnlyPlanUpdateResponses() {
        RestClusterClient<String> restClient = mock(RestClusterClient.class);
        AgentPlanClient client = new AgentPlanClient(restClient);
        JobID jobId = new JobID();
        CoordinationResponse unexpected = mock(CoordinationResponse.class);
        when(restClient.sendCoordinationRequest(
                        org.mockito.ArgumentMatchers.eq(jobId),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.any(PlanUpdateRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(unexpected));

        assertThatThrownBy(
                        () ->
                                client.updateAgentPlan(
                                        jobId.toHexString(), "review-agent", emptyPlanJson()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PlanUpdateResponse");
    }

    @SuppressWarnings("unchecked")
    @Test
    void rejectsBlankAgentNameBeforeSending() {
        RestClusterClient<String> restClient = mock(RestClusterClient.class);
        AgentPlanClient client = new AgentPlanClient(restClient);

        assertThatThrownBy(
                        () ->
                                client.updateAgentPlan(
                                        new JobID().toHexString(), " ", emptyPlanJson()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("agent name");
    }

    @SuppressWarnings("unchecked")
    @Test
    void closesUnderlyingRestClient() throws Exception {
        RestClusterClient<String> restClient = mock(RestClusterClient.class);
        AgentPlanClient client = new AgentPlanClient(restClient);

        client.close();

        verify(restClient).close();
    }

    @Test
    void rejectsBlankRestAddress() {
        assertThatThrownBy(() -> new AgentPlanClient(" ", 8081))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("REST address");
    }

    @Test
    void rejectsInvalidRestPort() {
        assertThatThrownBy(() -> new AgentPlanClient("localhost", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("REST port");
        assertThatThrownBy(() -> new AgentPlanClient("localhost", 65536))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("REST port");
    }

    private static String emptyPlanJson() throws Exception {
        return OBJECT_MAPPER.writeValueAsString(
                new AgentPlan(
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new AgentConfiguration()));
    }
}

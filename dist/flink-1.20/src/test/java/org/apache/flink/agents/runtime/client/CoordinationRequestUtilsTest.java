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

import org.apache.flink.agents.runtime.operator.coordinator.PlanUpdateMessages.PlanUpdateRequest;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoordinationRequestUtilsTest {

    @SuppressWarnings("unchecked")
    @Test
    void convertsUidToPrimaryOperatorIdOnFlinkOneTwenty() {
        RestClusterClient<String> restClient = mock(RestClusterClient.class);
        JobID jobId = new JobID();
        String operatorUid = "flink-agents-coordinated-operator";
        PlanUpdateRequest request = new PlanUpdateRequest("{}");
        CompletableFuture<CoordinationResponse> expected = new CompletableFuture<>();
        when(restClient.sendCoordinationRequest(
                        eq(jobId), org.mockito.ArgumentMatchers.any(OperatorID.class), eq(request)))
                .thenReturn(expected);

        CompletableFuture<CoordinationResponse> actual =
                CoordinationRequestUtils.send(restClient, jobId, operatorUid, request);

        assertThat(actual).isSameAs(expected);
        ArgumentCaptor<OperatorID> operatorIdCaptor = ArgumentCaptor.forClass(OperatorID.class);
        verify(restClient)
                .sendCoordinationRequest(eq(jobId), operatorIdCaptor.capture(), eq(request));
        assertThat(operatorIdCaptor.getValue().toHexString())
                .isEqualTo("3be5561e768cf5fc957e8c4b13acd9f9");
    }
}

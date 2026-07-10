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
package org.apache.flink.agents.runtime.operator.coordinator;

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/** Wire types for pushing AgentPlan updates through an {@code OperatorCoordinator}. */
public final class PlanUpdateMessages {

    private PlanUpdateMessages() {}

    /**
     * Coordinator → subtask: the plan to switch to. Carries the coordinator-minted sequence, the
     * content signature, and the canonical plan JSON so subtasks can verify integrity and apply
     * duplicate backfills idempotently.
     */
    public static final class PlanUpdateEvent implements OperatorEvent {
        private static final long serialVersionUID = 2L;
        public final long planVersion;
        public final String planId;
        public final String canonicalPlanJson;

        public PlanUpdateEvent(long planVersion, String planId, String canonicalPlanJson) {
            this.planVersion = planVersion;
            this.planId = planId;
            this.canonicalPlanJson = canonicalPlanJson;
        }
    }

    /** REST/CLI → coordinator: publish a new plan. */
    public static final class PlanUpdateRequest implements CoordinationRequest {
        private static final long serialVersionUID = 1L;
        public final String agentPlanJson;

        public PlanUpdateRequest(String agentPlanJson) {
            this.agentPlanJson = agentPlanJson;
        }
    }

    /**
     * Coordinator → caller: the minted sequence and planId (for publish confirmation), plus how
     * many subtasks the update was fanned out to.
     */
    public static final class PlanUpdateResponse implements CoordinationResponse {
        private static final long serialVersionUID = 2L;
        public final long planVersion;
        public final String planId;
        public final int subtasksNotified;

        public PlanUpdateResponse(long planVersion, String planId, int subtasksNotified) {
            this.planVersion = planVersion;
            this.planId = planId;
            this.subtasksNotified = subtasksNotified;
        }
    }
}

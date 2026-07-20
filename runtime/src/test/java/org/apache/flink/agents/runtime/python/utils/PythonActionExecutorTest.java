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
package org.apache.flink.agents.runtime.python.utils;

import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.PythonFunction;
import org.apache.flink.agents.plan.actions.Action;
import org.apache.flink.agents.runtime.python.context.PythonRunnerContextImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import pemja.core.PythonInterpreter;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PythonActionExecutorTest {

    @Test
    void frameworkCallsUseBoundModuleMethods() throws Exception {
        PythonInterpreter interpreter = mock(PythonInterpreter.class);
        Object output = new Object();
        when(interpreter.invokeMethod(
                        "__fa_java_utils_module", "get_output_from_output_event", "event-json"))
                .thenReturn(output);
        PythonActionExecutor executor = createExecutor(interpreter);

        assertThat(executor.getOutputFromOutputEvent("event-json")).isSameAs(output);
        verify(interpreter)
                .invokeMethod(
                        "__fa_java_utils_module", "get_output_from_output_event", "event-json");
    }

    @Test
    void openBindsReservedAndLegacyGlobalsWithoutProcessAliases() throws Exception {
        PythonInterpreter interpreter = mock(PythonInterpreter.class);
        PythonActionExecutor executor = createExecutor(interpreter);

        executor.open();

        ArgumentCaptor<String> imports = ArgumentCaptor.forClass(String.class);
        verify(interpreter).exec(imports.capture());
        assertThat(imports.getValue())
                .contains(
                        "from flink_agents.plan import function as __fa_function_module",
                        "function = __fa_function_module")
                .doesNotContain("sys.modules[");
    }

    @Test
    void openResolvesDeclaredActionsInsidePlanNamespace() throws Exception {
        PythonInterpreter interpreter = mock(PythonInterpreter.class);
        Action action =
                new Action(
                        "python-action",
                        new PythonFunction("app.agent", "handle"),
                        List.of(InputEvent.EVENT_TYPE));
        AgentPlan plan =
                new AgentPlan(
                        Map.of(action.getName(), action),
                        Map.of(InputEvent.EVENT_TYPE, List.of(action)));
        PythonActionExecutor executor = createExecutor(interpreter, plan);

        executor.open();

        verify(interpreter)
                .invokeMethod(
                        "__fa_function_module",
                        "resolve_python_function",
                        "app.agent",
                        "handle",
                        "__fa_job_operator_v2");
    }

    private static PythonActionExecutor createExecutor(PythonInterpreter interpreter)
            throws Exception {
        return createExecutor(interpreter, new AgentPlan(Map.of(), Map.of()));
    }

    private static PythonActionExecutor createExecutor(
            PythonInterpreter interpreter, AgentPlan agentPlan) throws Exception {
        return new PythonActionExecutor(
                interpreter,
                agentPlan,
                mock(JavaResourceAdapter.class),
                mock(PythonRunnerContextImpl.class),
                "job-1",
                "__fa_job_operator_v2");
    }
}

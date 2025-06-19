package org.apache.flink.agents.runtime;

import org.apache.flink.agents.plan.Action;
import org.apache.flink.agents.plan.ActionFunction;
import org.apache.flink.agents.plan.PythonFunction;
import org.apache.flink.agents.plan.WorkflowPlan;
import org.apache.flink.agents.runtime.message.DataMessage;
import org.apache.flink.agents.runtime.message.PythonDataMessage;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

public class FlinkAgentTest {
    @Test
    public void testFromDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        WorkflowPlan workflowPlan = new WorkflowPlan(new HashMap<>());

        Action fakeAction =
                new Action(
                        "fake_action",
                        new ActionFunction(
                                new PythonFunction(
                                        "flink_agents.api.test_action",
                                        "test_action.framework_wrapped_action")),
                        Collections.singletonList("flink_agents.api.event.InputEvent"));
        workflowPlan.addAction("flink_agents.api.event.InputEvent", fakeAction);

        DataStream<Long> pyflinkOutputDataStream = env.fromSequence(0, 1000000);
        DataStream<DataMessage<Long>> runtimeInputDataStream =
                pyflinkOutputDataStream
                        .map(
                                x ->
                                        (DataMessage<Long>)
                                                new PythonDataMessage<Long>(
                                                        "flink_agents.api.event.InputEvent",
                                                        x % 3,
                                                        x.toString().getBytes()))
                        .returns(new TypeHint<DataMessage<Long>>() {});
        ;

        DataStream<?> outputDataStream =
                FlinkAgent.fromDataStream(
                        runtimeInputDataStream, TypeInformation.of(Long.class), workflowPlan);
        outputDataStream.print();
        env.execute();
    }
}

package org.apache.flink.agents.runtime;

import org.apache.flink.agents.plan.Action;
import org.apache.flink.agents.plan.ActionFunction;
import org.apache.flink.agents.plan.PythonFunction;
import org.apache.flink.agents.plan.WorkflowPlan;
import org.apache.flink.agents.runtime.message.DataMessage;
import org.apache.flink.agents.runtime.message.PythonDataMessage;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

public class FlinkAgentTest {
    @Test
    public void testFromDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        WorkflowPlan workflowPlan = new WorkflowPlan(new HashMap<>());

        Action fakeAction1 =
                new Action(
                        "fake_action",
                        new ActionFunction(
                                new PythonFunction(
                                        "flink_agents.api.test_action",
                                        "test_action.framework_wrapped_action1")),
                        Collections.singletonList("flink_agents.api.event.InputEvent"));

        Action fakeAction2 =
                new Action(
                        "fake_action",
                        new ActionFunction(
                                new PythonFunction(
                                        "flink_agents.api.test_action",
                                        "test_action.framework_wrapped_action2")),
                        Collections.singletonList("flink_agents.api.event.TempEvent"));
        workflowPlan.addAction("flink_agents.api.event.InputEvent", fakeAction1);
        workflowPlan.addAction("flink_agents.api.event.TempEvent", fakeAction2);

        DataStream<Long> pyflinkOutputDataStream = env.fromSequence(0, 5);
        DataStream<DataMessage<Long>> runtimeInputDataStream =
                pyflinkOutputDataStream.flatMap(
                        new FlatMapFunction<Long, DataMessage<Long>>() {
                            @Override
                            public void flatMap(Long aLong, Collector<DataMessage<Long>> collector) throws Exception {
                                try {
                                    // 构造 PythonDataMessage 对象
                                    DataMessage<Long> message = new PythonDataMessage<>(
                                            "flink_agents.api.event.InputEvent",
                                            aLong % 3,
                                            aLong.toString().getBytes()
                                    );

                                    // 发送处理后的数据
                                    collector.collect(message);

                                    Thread.sleep(20_000);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException("Sleep interrupted", e);
                                }
                            }
                        }
                );

        DataStream<?> outputDataStream =
                FlinkAgent.fromDataStream(
                        runtimeInputDataStream, TypeInformation.of(Long.class), workflowPlan);
        outputDataStream.print();
        env.execute();
    }
}

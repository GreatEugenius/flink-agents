package org.apache.flink.agents.runtime.operator;

import org.apache.flink.agents.plan.WorkflowPlan;
import org.apache.flink.agents.runtime.message.DataMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.OutputTag;

public class FunctionGroupFactory<K>
        implements OneInputStreamOperatorFactory<DataMessage<K>, DataMessage<K>> {
    private final OutputTag<DataMessage<K>> outputTag;

    private final WorkflowPlan workflowPlan;

    private final TypeInformation<K> keyTypeInfo;

    private final TypeInformation<DataMessage<K>> dataMessageTypeInfo;

    public FunctionGroupFactory(
            OutputTag<DataMessage<K>> outputTag,
            WorkflowPlan workflowPlan,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<DataMessage<K>> dataMessageTypeInfo) {
        this.outputTag = outputTag;
        this.workflowPlan = workflowPlan;
        this.keyTypeInfo = keyTypeInfo;
        this.dataMessageTypeInfo = dataMessageTypeInfo;
    }

    public <T extends StreamOperator<DataMessage<K>>> T createStreamOperator(
            StreamOperatorParameters<DataMessage<K>> streamOperatorParameters) {
        FunctionGroupOperator fn =
                new FunctionGroupOperator(
                        outputTag,
                        streamOperatorParameters.getMailboxExecutor(),
                        ChainingStrategy.ALWAYS,
                        streamOperatorParameters.getProcessingTimeService(),
                        workflowPlan,
                        keyTypeInfo,
                        dataMessageTypeInfo);
        fn.setup(
                streamOperatorParameters.getContainingTask(),
                streamOperatorParameters.getStreamConfig(),
                streamOperatorParameters.getOutput());

        return (T) fn;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy chainingStrategy) {}

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return FeedbackOperator.class;
    }
}

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

import org.apache.flink.agents.plan.WorkflowPlan;
import org.apache.flink.agents.runtime.feedback.FeedbackKey;
import org.apache.flink.agents.runtime.message.DataMessage;
import org.apache.flink.agents.runtime.message.Message;
import org.apache.flink.agents.runtime.operator.FeedbackOperatorFactory;
import org.apache.flink.agents.runtime.operator.FeedbackSinkOperator;
import org.apache.flink.agents.runtime.operator.FunctionGroupFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.SerializableFunction;

/** h. */
public class FlinkAgent {

    private final StreamExecutionEnvironment env;

    public FlinkAgent(StreamExecutionEnvironment env) {
        this.env = env;
    }

    // input row contain two field, key and payload
    // output row only contain output payload
    public static <K> DataStream<DataMessage<K>> fromDataStream(
            DataStream<DataMessage<K>> inputDataStream,
            TypeInformation<K> keyTypeInfo,
            WorkflowPlan workflowPlan) {
        TypeInformation<DataMessage<K>> dataMessageTypeInfo =
                inputDataStream.getTransformation().getOutputType();
        FeedbackKey<Message> feedbackKey = new FeedbackKey<>("statefun-pipeline", 1L);
        DataStream<DataMessage<K>> feedbackOperator =
                inputDataStream
                        .keyBy(new DataMessageKeySelector(), keyTypeInfo)
                        .transform(
                                "feedback",
                                dataMessageTypeInfo,
                                new FeedbackOperatorFactory(
                                        feedbackKey, new DataMessageKeySelector()))
                        .returns(dataMessageTypeInfo);

        OutputTag<DataMessage<K>> outputTag = new OutputTag("feedback-output", dataMessageTypeInfo);
        SingleOutputStreamOperator<DataMessage<K>> functionGroupOperator =
                DataStreamUtils.reinterpretAsKeyedStream(
                                feedbackOperator, new DataMessageKeySelector(), keyTypeInfo)
                        .transform(
                                "function-group",
                                dataMessageTypeInfo,
                                new FunctionGroupFactory(
                                        outputTag, workflowPlan, keyTypeInfo, dataMessageTypeInfo))
                        .returns(dataMessageTypeInfo);

        FeedbackSinkOperator<K> sinkOperator = new FeedbackSinkOperator<>(feedbackKey);

        DataStream<Void> feedbackSink =
                functionGroupOperator
                        .keyBy(new DataMessageKeySelector(), keyTypeInfo)
                        .transform("feedback_sink", TypeInformation.of(Void.class), sinkOperator)
                        .returns(dataMessageTypeInfo);

        String stringKey = feedbackKey.asColocationKey();
        feedbackOperator.getTransformation().setCoLocationGroupKey(stringKey);
        functionGroupOperator.getTransformation().setCoLocationGroupKey(stringKey);
        feedbackSink.getTransformation().setCoLocationGroupKey(stringKey);

        feedbackOperator.getTransformation().setParallelism(functionGroupOperator.getParallelism());

        feedbackSink.getTransformation().setParallelism(functionGroupOperator.getParallelism());

        return functionGroupOperator.getSideOutput(outputTag);
    }

    private static final class DataMessageKeySelector<K>
            implements SerializableFunction<DataMessage<K>, K>, KeySelector<DataMessage<K>, K> {

        private static final long serialVersionUID = 1;

        @Override
        public K apply(DataMessage<K> message) {
            return getKey(message);
        }

        @Override
        public K getKey(DataMessage<K> dataMessage) {
            return dataMessage.getKey();
        }
    }
}

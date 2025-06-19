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

import org.apache.flink.agents.runtime.feedback.FeedbackKey;
import org.apache.flink.agents.runtime.message.DataMessage;
import org.apache.flink.agents.runtime.message.Message;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.function.SerializableFunction;

import java.util.Objects;

public final class FeedbackOperatorFactory<K>
        implements OneInputStreamOperatorFactory<DataMessage<K>, DataMessage> {

    private static final long serialVersionUID = 1;
    private final SerializableFunction<Message, ?> keySelector;
    private final FeedbackKey<Message> feedbackKey;

    public FeedbackOperatorFactory(
            FeedbackKey<Message> feedbackKey, SerializableFunction<Message, ?> keySelector) {
        this.feedbackKey = Objects.requireNonNull(feedbackKey);
        this.keySelector = Objects.requireNonNull(keySelector);
    }

    public <T extends StreamOperator<DataMessage>> T createStreamOperator(
            StreamOperatorParameters<DataMessage> streamOperatorParameters) {
        final TypeSerializer<Message> serializer =
                streamOperatorParameters
                        .getStreamConfig()
                        .getTypeSerializerIn(
                                0,
                                streamOperatorParameters
                                        .getContainingTask()
                                        .getUserCodeClassLoader());

        FeedbackOperator op =
                new FeedbackOperator(
                        feedbackKey,
                        keySelector,
                        // TODO: config value
                        1000L,
                        serializer,
                        streamOperatorParameters.getMailboxExecutor(),
                        streamOperatorParameters.getProcessingTimeService());

        op.setup(
                streamOperatorParameters.getContainingTask(),
                streamOperatorParameters.getStreamConfig(),
                streamOperatorParameters.getOutput());

        return (T) op;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy chainingStrategy) {
        // ignored
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return FeedbackOperator.class;
    }
}

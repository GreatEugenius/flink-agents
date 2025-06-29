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
package org.apache.flink.agents.runtime.logger;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

/**
 * NOTE: This source code was copied from the <a
 * href="https://github.com/apache/flink-statefun">flink-statefun</a>
 *
 * <p>The factory for creating unbounded feedback loggers.
 */
public final class UnboundedFeedbackLoggerFactory<T> {
    private final Supplier<KeyGroupStream<T>> supplier;
    private final ToIntFunction<T> keyGroupAssigner;
    private CheckpointedStreamOperations checkpointedStreamOperations;
    private final TypeSerializer<T> serializer;

    public UnboundedFeedbackLoggerFactory(
            Supplier<KeyGroupStream<T>> supplier,
            ToIntFunction<T> keyGroupAssigner,
            CheckpointedStreamOperations ops,
            TypeSerializer<T> serializer) {
        this.supplier = Objects.requireNonNull(supplier);
        this.keyGroupAssigner = Objects.requireNonNull(keyGroupAssigner);
        this.serializer = Objects.requireNonNull(serializer);
        this.checkpointedStreamOperations = Objects.requireNonNull(ops);
    }

    public UnboundedFeedbackLogger<T> create() {
        return new UnboundedFeedbackLogger<>(
                supplier, keyGroupAssigner, checkpointedStreamOperations, serializer);
    }

    public void setCheckpointedStreamOperations(CheckpointedStreamOperations ops) {
        this.checkpointedStreamOperations = ops;
    }
}

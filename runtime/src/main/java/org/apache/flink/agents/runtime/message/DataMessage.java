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

package org.apache.flink.agents.runtime.message;

import java.io.Serializable;
import java.util.OptionalLong;

/**
 * A special type used to wrap {@link org.apache.flink.agents.api.Event}, which must include an
 * event type, a key and the actual data. This type will be used for data transmission between Flink
 * operators.
 *
 * @param <K> The type of the key.
 */
public abstract class DataMessage<K> implements Message, Serializable {

    protected static final long serialVersionUID = 1L;

    protected final String eventType;

    protected final K key;

    public DataMessage(String eventType, K key) {
        this.eventType = eventType;
        this.key = key;
    }

    public String getEventType() {
        return eventType;
    }

    public K getKey() {
        return key;
    }

    public abstract Object getPayload();

    @Override
    public OptionalLong isBarrierMessage() {
        return OptionalLong.empty();
    }
}

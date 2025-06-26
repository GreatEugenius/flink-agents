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

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Specific message type for event data.
 *
 * @param <K> The type of the key.
 * @param <T> The type of the event.
 */
public class EventMessage<K, T> implements Message {

    private static final long serialVersionUID = 1L;
    protected final K key;
    protected final T event;
    protected final String eventType;

    public EventMessage(K key, T event, String eventType) {
        this.key = key;
        this.event = Objects.requireNonNull(event);
        this.eventType = Objects.requireNonNull(eventType);
    }

    public K getKey() {
        return key;
    }

    public T getEvent() {
        return event;
    }

    public String getEventType() {
        return eventType;
    }

    @Override
    public OptionalLong isBarrierMessage() {
        return OptionalLong.empty();
    }

    @Override
    public String toString() {
        return "EventMessage{" + "payload=" + event + '}';
    }
}

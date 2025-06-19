package org.apache.flink.agents.runtime.workflow;

import org.apache.flink.agents.api.Event;

public class PythonEvent extends Event {
    private String eventType;
    private byte[] key;
    private byte[] payload;

    public PythonEvent(String eventType, byte[] key, byte[] payload) {
        this.eventType = eventType;
        this.key = key;
        this.payload = payload;
    }

    public String getEventType() {
        return eventType;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getPayload() {
        return payload;
    }
}

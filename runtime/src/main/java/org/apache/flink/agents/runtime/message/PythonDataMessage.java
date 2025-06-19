package org.apache.flink.agents.runtime.message;

import java.util.Arrays;

public class PythonDataMessage<K> extends DataMessage<K> {

    private final byte[] payload;

    public PythonDataMessage(String eventType, K key, byte[] payload) {
        super(eventType, key);
        this.payload = payload;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "PythonDataMessage{"
                + "payload="
                + Arrays.toString(payload)
                + ", eventType='"
                + eventType
                + '\''
                + ", key="
                + key
                + '}';
    }
}

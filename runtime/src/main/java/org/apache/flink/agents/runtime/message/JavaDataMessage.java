package org.apache.flink.agents.runtime.message;

public class JavaDataMessage<K> extends DataMessage<K> {

    private final Object payload;

    public JavaDataMessage(String eventType, K key, Object payload) {
        super(eventType, key);
        this.payload = payload;
    }

    @Override
    public Object getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "JavaDataMessage{"
                + "payload="
                + payload
                + ", eventType='"
                + eventType
                + '\''
                + ", key="
                + key
                + '}';
    }
}

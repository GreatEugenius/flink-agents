package org.apache.flink.agents.runtime.context;

import org.apache.flink.agents.runtime.message.PythonDataMessage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class RunnerContext<K> {
    private K id;
    private Deque<PythonDataMessage<K>> events;

    public RunnerContext(K id) {
        this.id = id;
        this.events = new ArrayDeque<>();
    }

    public void sendEvent(String type, K key, byte[] event) {
        this.events.add(new PythonDataMessage<K>(type, key, event));
    }

    public K getSessionId() {
        return this.id;
    }

    public PythonDataMessage<K> getEvent() {
        System.out.println(this.events.size());
        return this.events.pollFirst();
    }

    // 暴露接口获取所有事件列表
    public List<PythonDataMessage<K>> getAllEvents() {
        List<PythonDataMessage<K>> list = new ArrayList<>(this.events);
        this.events.clear();
        return list;
    }
}

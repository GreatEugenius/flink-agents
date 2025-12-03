package org.apache.flink.agents.plan.resourceprovider;

import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.prompt.Prompt;
import org.apache.flink.agents.api.resource.Resource;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.plan.AgentPlan;
import pemja.core.PythonInterpreter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JavaResourceAdapter {
    private final Map<ResourceType, Map<String, ResourceProvider>> resourceProviders;

    private final transient PythonInterpreter interpreter;

    /** Cache for instantiated resources. */
    private transient Map<ResourceType, Map<String, Resource>> resourceCache;

    public JavaResourceAdapter(AgentPlan agentPlan, PythonInterpreter interpreter) {
        this.resourceProviders = agentPlan.getResourceProviders();
        this.interpreter = interpreter;
        this.resourceCache = new ConcurrentHashMap<>();
    }

    public Resource getResource(String name, String typeValue) throws Exception {
        ResourceType type = ResourceType.fromValue(typeValue);

        if (resourceCache.containsKey(type) && resourceCache.get(type).containsKey(name)) {
            return resourceCache.get(type).get(name);
        }

        if (!resourceProviders.containsKey(type)
                || !resourceProviders.get(type).containsKey(name)) {
            throw new IllegalArgumentException("Resource not found: " + name + " of type " + type);
        }

        ResourceProvider provider = resourceProviders.get(type).get(name);

        Resource resource =
                provider.provide(
                        (String anotherName, ResourceType anotherType) -> {
                            try {
                                return this.getResource(anotherName, anotherType);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        resourceCache.computeIfAbsent(type, k -> new ConcurrentHashMap<>()).put(name, resource);

        return resource;
    }

    public ChatMessage fromPythonChatMessage(Object pythonChatMessage) {
        ChatMessage chatMessage = new ChatMessage();
        if (interpreter == null) {
            throw new IllegalStateException("Python interpreter is not set.");
        }
        String roleValue =
                (String)
                        interpreter.invoke(
                                "python_java_utils.update_java_chat_message",
                                pythonChatMessage,
                                chatMessage);
        chatMessage.setRole(MessageRole.fromValue(roleValue));
        return chatMessage;
    }

    public Resource getResource(String name, ResourceType type) throws Exception {
        if (resourceCache.containsKey(type) && resourceCache.get(type).containsKey(name)) {
            return resourceCache.get(type).get(name);
        }

        if (!resourceProviders.containsKey(type)
                || !resourceProviders.get(type).containsKey(name)) {
            throw new IllegalArgumentException("Resource not found: " + name + " of type " + type);
        }

        ResourceProvider provider = resourceProviders.get(type).get(name);
        if (!(provider instanceof PythonResourceProvider)) {
            return provider.provide(
                    (String anotherName, ResourceType anotherType) -> {
                        try {
                            return this.getResource(anotherName, anotherType);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        if (type == ResourceType.PROMPT) {
            return getPythonPrompt(name);
        }
        Resource resource =
                provider.provide(
                        (String anotherName, ResourceType anotherType) -> {
                            try {
                                return this.getResource(anotherName, anotherType);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        // Cache the resource
        resourceCache.computeIfAbsent(type, k -> new ConcurrentHashMap<>()).put(name, resource);

        return resource;
    }

    private Prompt getPythonPrompt(String name) {
        PythonSerializableResourceProvider resourceProvider =
                (PythonSerializableResourceProvider)
                        resourceProviders.get(ResourceType.PROMPT).get(name);
        return (Prompt) resourceProvider.getResource();
    }
}

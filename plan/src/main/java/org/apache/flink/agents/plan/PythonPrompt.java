package org.apache.flink.agents.plan;

import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.prompt.Prompt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PythonPrompt extends Prompt {
    public PythonPrompt(String template) {
        super(template);
    }

    public PythonPrompt(List<ChatMessage> template) {
        super(template);
    }

    public static PythonPrompt fromSerializedMap(Map<String, Object> serialized) {
        if (serialized == null || !serialized.containsKey("template")) {
            throw new IllegalArgumentException("Map must contain 'template' key");
        }

        Object templateObj = serialized.get("template");
        if (!(templateObj instanceof List)) {
            throw new IllegalArgumentException("Template must be a List");
        }

        List<?> templateList = (List<?>) templateObj;
        if (templateList.isEmpty()) {
            throw new IllegalArgumentException("Template list cannot be empty");
        }

        List<ChatMessage> messages = new ArrayList<>();
        for (Object item : templateList) {
            if (!(item instanceof Map)) {
                throw new IllegalArgumentException("Each template item must be a Map");
            }

            Map<String, Object> messageMap = (Map<String, Object>) item;
            ChatMessage chatMessage = parseChatMessage(messageMap);
            messages.add(chatMessage);
        }

        return new PythonPrompt(messages);
    }

    /** Parse a single ChatMessage from a Map representation. */
    @SuppressWarnings("unchecked")
    private static ChatMessage parseChatMessage(Map<String, Object> messageMap) {
        // Extract role
        String roleValue = messageMap.get("role").toString();
        MessageRole role = MessageRole.fromValue(roleValue);

        // Extract content
        Object contentObj = messageMap.get("content");
        String content = contentObj != null ? contentObj.toString() : "";

        // Extract tool_calls (optional)
        List<Map<String, Object>> toolCalls =
                (List<Map<String, Object>>) messageMap.get("tool_calls");

        // Extract extra_args (optional)
        Map<String, Object> extraArgs = (Map<String, Object>) messageMap.get("extra_args");

        return new ChatMessage(role, content, toolCalls, extraArgs);
    }
}

package org.apache.flink.agents.plan;

import org.apache.flink.agents.api.tools.Tool;
import org.apache.flink.agents.api.tools.ToolMetadata;
import org.apache.flink.agents.api.tools.ToolParameters;
import org.apache.flink.agents.api.tools.ToolResponse;
import org.apache.flink.agents.api.tools.ToolType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class PythonTool extends Tool {
    protected PythonTool(ToolMetadata metadata) {
        super(metadata);
    }

    @SuppressWarnings("unchecked")
    public static PythonTool fromSerializedMap(Map<String, Object> serialized)
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> metadata = (Map<String, Object>) serialized.get("metadata");
        String name = (String) metadata.get("name");
        String description = (String) metadata.get("description");
        String inputSchema = mapper.writeValueAsString(metadata.get("args_schema"));
        return new PythonTool(new ToolMetadata(name, description, inputSchema));
    }

    @Override
    public ToolType getToolType() {
        return ToolType.REMOTE_FUNCTION;
    }

    @Override
    public ToolResponse call(ToolParameters parameters) {
        return null;
    }
}

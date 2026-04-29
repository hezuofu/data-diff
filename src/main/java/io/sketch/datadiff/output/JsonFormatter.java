package io.sketch.datadiff.output;

import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.spi.ResultFormatter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * JSON formatter for diff results using Jackson.
 */
public class JsonFormatter implements ResultFormatter {
    
    private final ObjectMapper mapper;
    
    public JsonFormatter() {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    
    @Override
    public void format(DiffResult result, OutputStream outputStream) {
        try {
            Map<String, Object> output = new HashMap<>();
            output.put("hasDifferences", result.hasDifferences());
            output.put("diffCount", result.getDiffCount());
            output.put("duration", result.getDuration().toString());
            output.put("diffCounts", result.getDiffCounts());
            output.put("diffRecords", result.getDiffRecords());
            
            mapper.writeValue(outputStream, output);
        } catch (Exception e) {
            throw new RuntimeException("Failed to format diff result as JSON", e);
        }
    }
    
    @Override
    public String getFormatName() {
        return "json";
    }
}

package io.sketch.datadiff.core.spi;

import io.sketch.datadiff.core.model.DiffResult;
import java.io.OutputStream;

/**
 * Service Provider Interface for formatting diff results.
 */
public interface ResultFormatter {
    
    /**
     * Format the diff result and write to output stream.
     * 
     * @param result diff result to format
     * @param outputStream output stream to write formatted result
     */
    void format(DiffResult result, OutputStream outputStream);
    
    /**
     * Get the format name (e.g., "json", "csv", "table").
     * 
     * @return format name
     */
    String getFormatName();
    
    /**
     * Format the diff result to a string.
     * 
     * @param result diff result to format
     * @return formatted string representation
     */
    default String formatToString(DiffResult result) {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        format(result, baos);
        return baos.toString(java.nio.charset.StandardCharsets.UTF_8);
    }
}

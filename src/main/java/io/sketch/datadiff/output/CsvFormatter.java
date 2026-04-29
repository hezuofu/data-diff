package io.sketch.datadiff.output;

import io.sketch.datadiff.core.model.DiffRecord;
import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.spi.ResultFormatter;

import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * CSV formatter for diff results.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class CsvFormatter implements ResultFormatter {
    
    private static final String DELIMITER = ",";
    private static final String QUOTE = "\"";
    
    @Override
    public void format(DiffResult result, OutputStream outputStream) {
        PrintWriter writer = new PrintWriter(outputStream);
        
        // Write header
        writer.println("primary_key,diff_type,left_value,right_value,differing_columns");
        
        // Write records
        for (DiffRecord record : result.getDiffRecords()) {
            writer.println(formatRecord(record));
        }
        
        writer.flush();
    }
    
    private String formatRecord(DiffRecord record) {
        String pk = escape(record.getPrimaryKeyString());
        String type = record.diffType().toString();
        String leftVal = record.leftData() != null ? escape(record.leftData().toString()) : "";
        String rightVal = record.rightData() != null ? escape(record.rightData().toString()) : "";
        String diffCols = String.join(";", record.differingColumns());
        
        return String.join(DELIMITER, pk, type, leftVal, rightVal, diffCols);
    }
    
    private String escape(String value) {
        if (value.contains(DELIMITER) || value.contains(QUOTE) || value.contains("\n")) {
            return QUOTE + value.replace(QUOTE, QUOTE + QUOTE) + QUOTE;
        }
        return value;
    }
    
    @Override
    public String getFormatName() {
        return "csv";
    }
}

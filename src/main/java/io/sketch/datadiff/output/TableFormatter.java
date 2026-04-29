package io.sketch.datadiff.output;

import io.sketch.datadiff.core.model.DiffRecord;
import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.spi.ResultFormatter;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Table formatter for console-friendly output.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class TableFormatter implements ResultFormatter {
    
    @Override
    public void format(DiffResult result, OutputStream outputStream) {
        PrintStream out = new PrintStream(outputStream);
        
        out.println("=".repeat(80));
        out.println("DATA DIFF RESULT");
        out.println("=".repeat(80));
        out.println();
        
        out.printf("Total Differences: %d%n", result.getDiffCount());
        out.printf("Duration: %s%n", result.getDuration());
        out.println();
        
        if (!result.hasDifferences()) {
            out.println("No differences found. Tables are identical.");
            return;
        }
        
        // Print diff counts by type
        out.println("Differences by Type:");
        result.getDiffCounts().forEach((type, count) -> 
            out.printf("  %-12s: %d%n", type, count)
        );
        out.println();
        
        // Print diff records
        out.println("-".repeat(80));
        out.printf("%-15s | %-12s | %-20s%n", "Primary Key", "Type", "Differing Columns");
        out.println("-".repeat(80));
        
        for (DiffRecord record : result.getDiffRecords()) {
            String pk = record.getPrimaryKeyString();
            String type = record.diffType().toString();
            String cols = record.differingColumns().isEmpty() ? 
                "N/A" : String.join(", ", record.differingColumns());
            
            out.printf("%-15s | %-12s | %-20s%n", pk, type, cols);
        }
        
        out.println("=".repeat(80));
    }
    
    @Override
    public String getFormatName() {
        return "table";
    }
}

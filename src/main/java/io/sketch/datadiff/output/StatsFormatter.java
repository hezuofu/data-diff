package io.sketch.datadiff.output;

import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.spi.ResultFormatter;

import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * Statistics formatter for comparison summary.
 */
public class StatsFormatter implements ResultFormatter {
    
    @Override
    public void format(DiffResult result, OutputStream outputStream) {
        PrintWriter writer = new PrintWriter(outputStream);
        
        writer.println("=== Data Diff Statistics ===");
        writer.println();
        writer.printf("Comparison Duration: %s%n", result.getDuration());
        writer.printf("Total Differences: %d%n", result.getDiffCount());
        writer.println();
        
        writer.println("Differences by Type:");
        result.getDiffCounts().forEach((type, count) -> 
            writer.printf("  %-15s: %d%n", type.toString(), count)
        );
        writer.println();
        
        DiffResult.Statistics stats = result.getStatistics();
        writer.println("Comparison Statistics:");
        writer.printf("  Left Row Count: %d%n", stats.leftRowCount());
        writer.printf("  Right Row Count: %d%n", stats.rightRowCount());
        writer.printf("  Segments Compared: %d%n", stats.segmentsCompared());
        writer.printf("  Bisection Iterations: %d%n", stats.bisectionIterations());
        writer.printf("  Max Bisection Depth: %d%n", stats.maxBisectionDepth());
        
        writer.flush();
    }
    
    @Override
    public String getFormatName() {
        return "stats";
    }
}

package io.sketch.datadiff.example;

import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.core.model.ColumnDef;
import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.TableInfo;

import java.util.List;
import java.util.Set;

/**
 * Example demonstrating DataDiff usage.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class DataDiffExample {
    
    public static void main(String[] args) {
        // Example 1: Basic comparison
        basicComparison();
        
        // Example 2: Advanced configuration
        advancedComparison();
    }
    
    /**
     * Basic comparison example.
     */
    private static void basicComparison() {
        System.out.println("=== Basic Comparison Example ===\n");
        
        // Setup data sources (replace with your actual database credentials)
        // Properties props = new Properties();
        // props.setProperty("user", "root");
        // props.setProperty("password", "password");
        // 
        // DataSource leftDs = HikariCPProvider.createDataSource(
        //     "jdbc:mysql://localhost:3306/source_db", props
        // );
        // DataSource rightDs = HikariCPProvider.createDataSource(
        //     "jdbc:mysql://localhost:3306/target_db", props
        // );
        
        // For demonstration, we'll skip actual execution
        System.out.println("This example shows how to configure DataDiffEngine");
        System.out.println("Replace the commented code with your actual database connections\n");
        
        // Define table schema
        TableInfo usersTable = new TableInfo(
            "users",
            List.of(
                new ColumnDef("id", "BIGINT", false),
                new ColumnDef("name", "VARCHAR(100)", false),
                new ColumnDef("email", "VARCHAR(255)", true),
                new ColumnDef("age", "INTEGER", true),
                new ColumnDef("created_at", "TIMESTAMP", false)
            ),
            List.of("id")
        );
        
        // Build engine with default options
        // DataDiffEngine engine = DataDiffEngine.builder()
        //     .leftDataSource(leftDs)
        //     .rightDataSource(rightDs)
        //     .build();
        
        // Execute comparison
        // DiffResult result = engine.compare(usersTable, usersTable);
        
        // Output results
        // if (result.hasDifferences()) {
        //     System.out.println("Found " + result.getDiffCount() + " differences");
        //     new TableFormatter().format(result, System.out);
        // } else {
        //     System.out.println("Tables are identical!");
        // }
    }
    
    /**
     * Advanced configuration example.
     */
    private static void advancedComparison() {
        System.out.println("\n=== Advanced Configuration Example ===\n");
        
        // Advanced options
        CompareOptions options = CompareOptions.builder()
            .algorithm(CompareOptions.Algorithm.HASHDIFF)
            .segmentSize(100000)                // 100K rows per segment
            .parallelism(16)                    // Use 16 virtual threads
            .maxBisectionDepth(32)              // Max recursion depth
            .excludeColumns("created_at", "updated_at")  // Ignore audit columns
            .caseInsensitiveColumns(Set.of("email", "name"))  // Case-insensitive
            .numericTolerance(0.001)            // Allow 0.001 difference for floats
            .useChecksumCache(true)             // Enable caching
            .cacheMaxSize(50000)                // Cache up to 50K segments
            .build();
        
        System.out.println("Configuration:");
        System.out.println("  Algorithm: " + options.getAlgorithm());
        System.out.println("  Segment Size: " + options.getSegmentSize());
        System.out.println("  Parallelism: " + options.getParallelism());
        System.out.println("  Excluded Columns: " + options.getExcludeColumns());
        System.out.println("  Case-Insensitive: " + options.getCaseInsensitiveColumns());
        System.out.println("  Numeric Tolerance: " + options.getNumericTolerance());
        System.out.println();
        
        // Usage:
        // DataDiffEngine engine = DataDiffEngine.builder()
        //     .leftDataSource(leftDs)
        //     .rightDataSource(rightDs)
        //     .options(options)
        //     .build();
        // 
        // DiffResult result = engine.compare(leftTable, rightTable);
        // 
        // // JSON output
        // new JsonFormatter().format(result, System.out);
    }
    
    /**
     * Example: Cross-database comparison.
     */
    private static void crossDatabaseComparison() {
        System.out.println("\n=== Cross-Database Comparison Example ===\n");
        
        // Compare MySQL to PostgreSQL
        // Properties mysqlProps = new Properties();
        // mysqlProps.setProperty("user", "root");
        // mysqlProps.setProperty("password", "password");
        // DataSource mysqlDs = HikariCPProvider.createDataSource(
        //     "jdbc:mysql://mysql-host:3306/mydb", mysqlProps
        // );
        // 
        // Properties pgProps = new Properties();
        // pgProps.setProperty("user", "postgres");
        // pgProps.setProperty("password", "password");
        // DataSource pgDs = HikariCPProvider.createDataSource(
        //     "jdbc:postgresql://pg-host:5432/mydb", pgProps
        // );
        // 
        // DataDiffEngine engine = DataDiffEngine.builder()
        //     .leftDataSource(mysqlDs)
        //     .rightDataSource(pgDs)
        //     .options(CompareOptions.builder()
        //         .algorithm(CompareOptions.Algorithm.HASHDIFF)  // Must use HASHDIFF for cross-db
        //         .build())
        //     .build();
        
        System.out.println("Cross-database comparison requires HashDiff algorithm");
        System.out.println("The framework automatically detects database dialects");
    }
    
    /**
     * Example: JoinDiff for same-database comparison.
     */
    private static void joinDiffExample() {
        System.out.println("\n=== JoinDiff Example (Same Database) ===\n");
        
        // For tables in the same database, JoinDiff is faster
        // DataSource ds = HikariCPProvider.createDataSource(
        //     "jdbc:mysql://localhost:3306/mydb", props
        // );
        // 
        // DataDiffEngine engine = DataDiffEngine.builder()
        //     .bothDataSource(ds)  // Same data source for both
        //     .options(CompareOptions.builder()
        //         .algorithm(CompareOptions.Algorithm.JOINDIFF)  // Use JOIN
        //         .build())
        //     .build();
        // 
        // DiffResult result = engine.compare(oldVersionTable, newVersionTable);
        
        System.out.println("JoinDiff uses FULL OUTER JOIN for faster same-db comparison");
        System.out.println("Recommended when both tables are in the same database");
    }
}

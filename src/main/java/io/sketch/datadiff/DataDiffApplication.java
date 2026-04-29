package io.sketch.datadiff;

import io.sketch.datadiff.config.AppConfig;
import io.sketch.datadiff.config.ConfigLoader;
import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.core.spi.ResultFormatter;
import io.sketch.datadiff.engine.DataDiffEngine;
import io.sketch.datadiff.engine.HashDiffEngine;
import io.sketch.datadiff.engine.JoinDiffEngine;
import io.sketch.datadiff.output.CsvFormatter;
import io.sketch.datadiff.output.JsonFormatter;
import io.sketch.datadiff.output.StatsFormatter;
import io.sketch.datadiff.output.TableFormatter;
import io.sketch.datadiff.datasource.pool.HikariCPProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Main application entry point with configuration file support.
 * 
 * Usage:
 *   java -jar data-diff.jar                                    # Uses default application.yaml
 *   java -jar data-diff.jar --config my-config.yaml           # Uses custom config file
 *   java -jar data-diff.jar --help                             # Shows help
 * 
 * Author: lanxia39@163.com
 */
public class DataDiffApplication {
    
    private static final Logger log = LoggerFactory.getLogger(DataDiffApplication.class);
    
    public static void main(String[] args) {
        try {
            // Parse command line arguments
            CommandLineArgs cliArgs = parseArgs(args);
            
            if (cliArgs.showHelp) {
                printHelp();
                return;
            }
            
            // Load configuration
            AppConfig config = ConfigLoader.load(cliArgs.configPath);
            
            // Execute comparison
            run(config);
            
        } catch (Exception e) {
            log.error("Application failed: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    
    /**
     * Run data comparison with given configuration.
     */
    public static void run(AppConfig config) throws Exception {
        log.info("Starting Data Diff comparison...");
        log.info("Algorithm: {}", config.getComparison().getAlgorithm());
        log.info("Left table: {} at {}", config.getLeft().getTable(), config.getLeft().getUrl());
        log.info("Right table: {} at {}", config.getRight().getTable(), config.getRight().getUrl());
        
        // Create data sources
        DataSource leftDataSource = createDataSource(config.getLeft());
        DataSource rightDataSource = createDataSource(config.getRight());
        
        // Build table info
        TableInfo leftTable = buildTableInfo(config.getLeft());
        TableInfo rightTable = buildTableInfo(config.getRight());
        
        // Build comparison options
        CompareOptions options = buildOptions(config);
        
        // Create strategy based on algorithm
        boolean useHashDiff = "hashdiff".equalsIgnoreCase(config.getComparison().getAlgorithm());
        var strategy = useHashDiff 
            ? new HashDiffEngine() 
            : new JoinDiffEngine();
        
        // Create and execute engine
        DataDiffEngine engine = DataDiffEngine.create(leftDataSource, rightDataSource, strategy);
        DiffResult result = engine.compare(leftTable, rightTable, options);
        
        // Output results
        outputResult(result, config);
        
        log.info("Data Diff completed. Found {} differences.", result.getDiffCount());
    }
    
    /**
     * Create DataSource from database config.
     */
    private static DataSource createDataSource(AppConfig.DatabaseConfig dbConfig) {
        Properties props = new Properties();
        props.setProperty("user", dbConfig.getUsername());
        props.setProperty("password", dbConfig.getPassword());
        
        return HikariCPProvider.createDataSource(dbConfig.getUrl(), props);
    }
    
    /**
     * Build TableInfo from config.
     */
    private static TableInfo buildTableInfo(AppConfig.DatabaseConfig dbConfig) {
        // Note: In production, you would fetch schema from database
        // For now, create a minimal TableInfo
        List<String> primaryKey = dbConfig.getPrimaryKey() != null ? 
            dbConfig.getPrimaryKey() : List.of("id");
        
        return new TableInfo(
            dbConfig.getTable(),
            List.of(), // Columns will be auto-detected
            primaryKey
        );
    }
    
    /**
     * Build CompareOptions from config.
     */
    private static CompareOptions buildOptions(AppConfig config) {
        CompareOptions.Builder builder = CompareOptions.builder()
            .algorithm(CompareOptions.Algorithm.valueOf(
                config.getComparison().getAlgorithm().toUpperCase()))
            .segmentSize(config.getComparison().getSegmentSize())
            .parallelism(config.getComparison().getParallelism())
            .maxBisectionDepth(config.getComparison().getMaxBisectionDepth())
            .numericTolerance(config.getComparison().getNumericTolerance());
        
        if (config.getLeft().getExcludeColumns() != null) {
            builder.excludeColumns(config.getLeft().getExcludeColumns().toArray(new String[0]));
        }
        
        return builder.build();
    }
    
    /**
     * Output diff result.
     */
    private static void outputResult(DiffResult result, AppConfig config) throws Exception {
        ResultFormatter formatter = createFormatter(config.getOutput().getFormat());
        
        String outputFile = config.getOutput().getOutputFile();
        
        try (OutputStream os = outputFile != null 
                ? new FileOutputStream(outputFile) 
                : System.out) {
            
            if (outputFile != null) {
                // Ensure parent directory exists
                Files.createDirectories(Paths.get(outputFile).getParent());
                log.info("Writing results to: {}", outputFile);
            }
            
            formatter.format(result, os);
        }
        
        // Show stats if enabled
        if (config.getOutput().isShowStats()) {
            StatsFormatter statsFormatter = new StatsFormatter();
            statsFormatter.format(result, System.out);
        }
    }
    
    /**
     * Create formatter based on format name.
     */
    private static ResultFormatter createFormatter(String format) {
        return switch (format.toLowerCase()) {
            case "json" -> new JsonFormatter();
            case "csv" -> new CsvFormatter();
            case "stats" -> new StatsFormatter();
            case "table" -> new TableFormatter();
            default -> throw new IllegalArgumentException("Unsupported format: " + format);
        };
    }
    
    /**
     * Parse command line arguments.
     */
    private static CommandLineArgs parseArgs(String[] args) {
        CommandLineArgs cliArgs = new CommandLineArgs();
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config", "-c" -> {
                    if (i + 1 < args.length) {
                        cliArgs.configPath = args[++i];
                    } else {
                        throw new IllegalArgumentException("Missing config file path");
                    }
                }
                case "--help", "-h" -> cliArgs.showHelp = true;
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        
        return cliArgs;
    }
    
    /**
     * Print help message.
     */
    private static void printHelp() {
        System.out.println("Data Diff - Database Comparison Tool");
        System.out.println("Author: lanxia39@163.com");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar data-diff.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --config, -c <file>   Configuration file path (default: application.yaml)");
        System.out.println("  --help, -h            Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar data-diff.jar");
        System.out.println("  java -jar data-diff.jar --config my-config.yaml");
        System.out.println();
        System.out.println("Configuration file format: YAML");
        System.out.println("See application.yaml for example configuration.");
    }
    
    /**
     * Command line arguments holder.
     */
    private static class CommandLineArgs {
        String configPath = null;
        boolean showHelp = false;
    }
}

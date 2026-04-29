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
        log.info("Algorithm: {}", config.comparison().algorithm());
        log.info("Left table: {} at {}", config.left().table(), config.left().url());
        log.info("Right table: {} at {}", config.right().table(), config.right().url());
        
        // Create data sources
        DataSource leftDataSource = createDataSource(config.left());
        DataSource rightDataSource = createDataSource(config.right());
        
        // Build table info
        TableInfo leftTable = buildTableInfo(config.left());
        TableInfo rightTable = buildTableInfo(config.right());
        
        // Build comparison options
        CompareOptions options = buildOptions(config);
        
        // Create strategy based on algorithm
        boolean useHashDiff = "hashdiff".equalsIgnoreCase(config.comparison().algorithm());
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
        props.setProperty("user", dbConfig.username());
        props.setProperty("password", dbConfig.password());
        
        return HikariCPProvider.createDataSource(dbConfig.url(), props);
    }
    
    /**
     * Build TableInfo from config.
     */
    private static TableInfo buildTableInfo(AppConfig.DatabaseConfig dbConfig) {
        // Note: In production, you would fetch schema from database
        // For now, create a minimal TableInfo
        List<String> primaryKey = dbConfig.primaryKey();
        
        return new TableInfo(
            dbConfig.table(),
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
                config.comparison().algorithm().toUpperCase()))
            .segmentSize(config.comparison().segmentSize())
            .parallelism(config.comparison().parallelism())
            .maxBisectionDepth(config.comparison().maxBisectionDepth())
            .numericTolerance(config.comparison().numericTolerance());
        
        List<String> excludeColumns = config.left().excludeColumns();
        if (!excludeColumns.isEmpty()) {
            builder.excludeColumns(excludeColumns.toArray(new String[0]));
        }
        
        return builder.build();
    }
    
    /**
     * Output diff result.
     */
    private static void outputResult(DiffResult result, AppConfig config) throws Exception {
        ResultFormatter formatter = createFormatter(config.output().format());
        
        String outputFile = config.output().outputFile();
        
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
        if (config.output().showStats()) {
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
        System.out.println("  --config, -c <file>   Configuration file path (default: application.conf)");
        System.out.println("  --help, -h            Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar data-diff.jar");
        System.out.println("  java -jar data-diff.jar --config my-config.conf");
        System.out.println();
        System.out.println("Configuration file format: HOCON (Human-Optimized Config Object Notation)");
        System.out.println("See application.conf for example configuration.");
    }
    
    /**
     * Command line arguments holder.
     */
    private static class CommandLineArgs {
        String configPath = null;
        boolean showHelp = false;
    }
}

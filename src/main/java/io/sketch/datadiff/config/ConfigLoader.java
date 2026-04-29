package io.sketch.datadiff.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration loader for YAML files.
 * 
 * Author: lanxia39@163.com
 */
public class ConfigLoader {
    
    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
    private static final String DEFAULT_CONFIG = "application.yaml";
    
    /**
     * Load configuration from file path.
     * 
     * @param configPath Path to YAML configuration file
     * @return AppConfig instance
     */
    public static AppConfig load(String configPath) {
        Path path = configPath != null ? Paths.get(configPath) : Paths.get(DEFAULT_CONFIG);
        
        if (!Files.exists(path)) {
            throw new IllegalArgumentException(
                String.format("Configuration file not found: %s", path.toAbsolutePath())
            );
        }
        
        log.info("Loading configuration from: {}", path.toAbsolutePath());
        
        try (InputStream input = new FileInputStream(path.toFile())) {
            Yaml yaml = new Yaml();
            AppConfig config = yaml.loadAs(input, AppConfig.class);
            
            if (config == null) {
                throw new IllegalStateException("Failed to parse configuration file");
            }
            
            validate(config);
            log.info("Configuration loaded successfully");
            return config;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration file", e);
        }
    }
    
    /**
     * Load configuration from default location (application.yaml).
     * 
     * @return AppConfig instance
     */
    public static AppConfig loadDefault() {
        return load(null);
    }
    
    /**
     * Validate configuration.
     */
    private static void validate(AppConfig config) {
        if (config.getLeft() == null || config.getRight() == null) {
            throw new IllegalStateException("Both 'left' and 'right' database configurations are required");
        }
        
        if (config.getLeft().getUrl() == null || config.getRight().getUrl() == null) {
            throw new IllegalStateException("Database URL is required for both left and right databases");
        }
        
        if (config.getLeft().getTable() == null) {
            throw new IllegalStateException("Table name is required for left database");
        }
        
        if (config.getComparison() == null) {
            config.setComparison(new AppConfig.ComparisonConfig());
        }
        
        if (config.getOutput() == null) {
            config.setOutput(new AppConfig.OutputConfig());
        }
        
        // Validate algorithm
        String algorithm = config.getComparison().getAlgorithm().toLowerCase();
        if (!algorithm.equals("hashdiff") && !algorithm.equals("joindiff")) {
            throw new IllegalArgumentException(
                String.format("Unsupported algorithm: %s. Must be 'hashdiff' or 'joindiff'", algorithm)
            );
        }
        
        // Validate output format
        String format = config.getOutput().getFormat().toLowerCase();
        if (!format.equals("json") && !format.equals("csv") && 
            !format.equals("table") && !format.equals("stats")) {
            throw new IllegalArgumentException(
                String.format("Unsupported output format: %s. Must be 'json', 'csv', 'table', or 'stats'", format)
            );
        }
    }
}

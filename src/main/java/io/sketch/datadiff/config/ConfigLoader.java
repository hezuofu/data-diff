package io.sketch.datadiff.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Configuration loader facade for AppConfig.
 * Provides convenient loading methods with validation.
 * 
 * Author: lanxia39@163.com
 */
public class ConfigLoader {
    
    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
    
    /**
     * Load configuration from file path.
     * 
     * @param configPath Path to HOCON configuration file
     * @return AppConfig instance
     */
    public static AppConfig load(String configPath) {
        if (configPath == null || configPath.isEmpty()) {
            log.info("Loading default configuration (application.conf)");
            return AppConfig.load();
        }
        
        File file = new File(configPath);
        if (!file.exists()) {
            throw new IllegalArgumentException(
                String.format("Configuration file not found: %s", file.getAbsolutePath())
            );
        }
        
        log.info("Loading configuration from: {}", file.getAbsolutePath());
        AppConfig config = AppConfig.load(file);
        
        validate(config);
        log.info("Configuration loaded successfully");
        
        return config;
    }
    
    /**
     * Load configuration from default location.
     */
    public static AppConfig loadDefault() {
        return load(null);
    }
    
    /**
     * Validate configuration.
     */
    private static void validate(AppConfig config) {
        // Validate database configurations
        try {
            config.left().url();
            config.left().table();
        } catch (Exception e) {
            throw new IllegalStateException("Left database configuration is incomplete: " + e.getMessage());
        }
        
        try {
            config.right().url();
        } catch (Exception e) {
            throw new IllegalStateException("Right database configuration is incomplete: " + e.getMessage());
        }
        
        // Validate algorithm
        String algorithm = config.comparison().algorithm().toLowerCase();
        if (!algorithm.equals("hashdiff") && !algorithm.equals("joindiff")) {
            throw new IllegalArgumentException(
                String.format("Unsupported algorithm: %s. Must be 'hashdiff' or 'joindiff'", algorithm)
            );
        }
        
        // Validate output format
        String format = config.output().format().toLowerCase();
        if (!format.equals("json") && !format.equals("csv") && 
            !format.equals("table") && !format.equals("stats")) {
            throw new IllegalArgumentException(
                String.format("Unsupported output format: %s. Must be 'json', 'csv', 'table', or 'stats'", format)
            );
        }
    }
}

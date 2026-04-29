package io.sketch.datadiff.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Typesafe configuration wrapper for application settings.
 * Provides type-safe access to configuration values.
 * 
 * Author: lanxia39@163.com
 */
public class AppConfig {
    
    private final Config config;
    
    private AppConfig(Config config) {
        this.config = config;
    }
    
    /**
     * Load configuration from default location (application.conf).
     */
    public static AppConfig load() {
        Config config = ConfigFactory.load();
        return new AppConfig(config);
    }
    
    /**
     * Load configuration from specific file.
     */
    public static AppConfig load(File file) {
        Config config = ConfigFactory.parseFile(file)
            .withFallback(ConfigFactory.load());
        return new AppConfig(config);
    }
    
    /**
     * Load configuration from file path.
     */
    public static AppConfig load(String filePath) {
        return load(new File(filePath));
    }
    
    // ========== Database Configuration ==========
    
    public DatabaseConfig left() {
        return new DatabaseConfig(config.getConfig("left"));
    }
    
    public DatabaseConfig right() {
        return new DatabaseConfig(config.getConfig("right"));
    }
    
    public static class DatabaseConfig {
        private final Config config;
        
        DatabaseConfig(Config config) {
            this.config = config;
        }
        
        public String url() {
            return config.getString("url");
        }
        
        public String username() {
            return config.hasPath("username") ? config.getString("username") : "";
        }
        
        public String password() {
            return config.hasPath("password") ? config.getString("password") : "";
        }
        
        public String driver() {
            return config.hasPath("driver") ? config.getString("driver") : "";
        }
        
        public String table() {
            return config.getString("table");
        }
        
        public List<String> primaryKey() {
            return config.hasPath("primaryKey") 
                ? config.getStringList("primaryKey") 
                : List.of("id");
        }
        
        public List<String> excludeColumns() {
            return config.hasPath("excludeColumns") 
                ? config.getStringList("excludeColumns") 
                : List.of();
        }
        
        public int maxConnections() {
            return config.hasPath("maxConnections") 
                ? config.getInt("maxConnections") 
                : 10;
        }
    }
    
    // ========== Comparison Configuration ==========
    
    public ComparisonConfig comparison() {
        return config.hasPath("comparison") 
            ? new ComparisonConfig(config.getConfig("comparison"))
            : new ComparisonConfig(ConfigFactory.empty());
    }
    
    public static class ComparisonConfig {
        private final Config config;
        
        ComparisonConfig(Config config) {
            this.config = config;
        }
        
        public String algorithm() {
            return config.hasPath("algorithm") 
                ? config.getString("algorithm") 
                : "hashdiff";
        }
        
        public int segmentSize() {
            return config.hasPath("segmentSize") 
                ? config.getInt("segmentSize") 
                : 50000;
        }
        
        public int parallelism() {
            return config.hasPath("parallelism") 
                ? config.getInt("parallelism") 
                : 4;
        }
        
        public int maxBisectionDepth() {
            return config.hasPath("maxBisectionDepth") 
                ? config.getInt("maxBisectionDepth") 
                : 10;
        }
        
        public double numericTolerance() {
            return config.hasPath("numericTolerance") 
                ? config.getDouble("numericTolerance") 
                : 0.0;
        }
        
        public boolean caseInsensitive() {
            return config.hasPath("caseInsensitive") 
                ? config.getBoolean("caseInsensitive") 
                : false;
        }
        
        public Map<String, String> customComparators() {
            if (!config.hasPath("customComparators")) {
                return Map.of();
            }
            return config.getObject("customComparators").entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().unwrapped().toString()
                ));
        }
    }
    
    // ========== Output Configuration ==========
    
    public OutputConfig output() {
        return config.hasPath("output") 
            ? new OutputConfig(config.getConfig("output"))
            : new OutputConfig(ConfigFactory.empty());
    }
    
    public static class OutputConfig {
        private final Config config;
        
        OutputConfig(Config config) {
            this.config = config;
        }
        
        public String format() {
            return config.hasPath("format") 
                ? config.getString("format") 
                : "json";
        }
        
        public String outputFile() {
            return config.hasPath("outputFile") 
                ? config.getString("outputFile") 
                : null;
        }
        
        public boolean showStats() {
            return config.hasPath("showStats") 
                ? config.getBoolean("showStats") 
                : true;
        }
        
        public int maxRecords() {
            return config.hasPath("maxRecords") 
                ? config.getInt("maxRecords") 
                : 1000;
        }
    }
    
    /**
     * Get underlying Config object for advanced usage.
     */
    public Config underlying() {
        return config;
    }
}

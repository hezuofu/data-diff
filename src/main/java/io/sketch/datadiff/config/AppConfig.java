package io.sketch.datadiff.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Application configuration model.
 * 
 * Author: lanxia39@163.com
 */
public class AppConfig {
    
    private DatabaseConfig left;
    private DatabaseConfig right;
    private ComparisonConfig comparison;
    private OutputConfig output;
    
    public static class DatabaseConfig {
        private String url;
        private String username;
        private String password;
        private String driver;
        private String table;
        private List<String> primaryKey;
        private List<String> excludeColumns;
        private int maxConnections = 10;
        
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        
        public String getDriver() { return driver; }
        public void setDriver(String driver) { this.driver = driver; }
        
        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }
        
        public List<String> getPrimaryKey() { return primaryKey; }
        public void setPrimaryKey(List<String> primaryKey) { this.primaryKey = primaryKey; }
        
        public List<String> getExcludeColumns() { return excludeColumns; }
        public void setExcludeColumns(List<String> excludeColumns) { this.excludeColumns = excludeColumns; }
        
        public int getMaxConnections() { return maxConnections; }
        public void setMaxConnections(int maxConnections) { this.maxConnections = maxConnections; }
    }
    
    public static class ComparisonConfig {
        private String algorithm = "hashdiff";
        private int segmentSize = 50000;
        private int parallelism = 4;
        private int maxBisectionDepth = 10;
        private double numericTolerance = 0.0;
        private boolean caseInsensitive = false;
        private Map<String, String> customComparators = new HashMap<>();
        
        public String getAlgorithm() { return algorithm; }
        public void setAlgorithm(String algorithm) { this.algorithm = algorithm; }
        
        public int getSegmentSize() { return segmentSize; }
        public void setSegmentSize(int segmentSize) { this.segmentSize = segmentSize; }
        
        public int getParallelism() { return parallelism; }
        public void setParallelism(int parallelism) { this.parallelism = parallelism; }
        
        public int getMaxBisectionDepth() { return maxBisectionDepth; }
        public void setMaxBisectionDepth(int maxBisectionDepth) { this.maxBisectionDepth = maxBisectionDepth; }
        
        public double getNumericTolerance() { return numericTolerance; }
        public void setNumericTolerance(double numericTolerance) { this.numericTolerance = numericTolerance; }
        
        public boolean isCaseInsensitive() { return caseInsensitive; }
        public void setCaseInsensitive(boolean caseInsensitive) { this.caseInsensitive = caseInsensitive; }
        
        public Map<String, String> getCustomComparators() { return customComparators; }
        public void setCustomComparators(Map<String, String> customComparators) { this.customComparators = customComparators; }
    }
    
    public static class OutputConfig {
        private String format = "json";
        private String outputFile;
        private boolean showStats = true;
        private int maxRecords = 1000;
        
        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
        
        public String getOutputFile() { return outputFile; }
        public void setOutputFile(String outputFile) { this.outputFile = outputFile; }
        
        public boolean isShowStats() { return showStats; }
        public void setShowStats(boolean showStats) { this.showStats = showStats; }
        
        public int getMaxRecords() { return maxRecords; }
        public void setMaxRecords(int maxRecords) { this.maxRecords = maxRecords; }
    }
    
    public DatabaseConfig getLeft() { return left; }
    public void setLeft(DatabaseConfig left) { this.left = left; }
    
    public DatabaseConfig getRight() { return right; }
    public void setRight(DatabaseConfig right) { this.right = right; }
    
    public ComparisonConfig getComparison() { return comparison; }
    public void setComparison(ComparisonConfig comparison) { this.comparison = comparison; }
    
    public OutputConfig getOutput() { return output; }
    public void setOutput(OutputConfig output) { this.output = output; }
}

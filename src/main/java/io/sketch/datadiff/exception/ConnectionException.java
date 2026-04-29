package io.sketch.datadiff.exception;

/**
 * Exception thrown when database connection fails.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class ConnectionException extends DataDiffException {
    
    private final String dataSourceName;
    
    public ConnectionException(String dataSourceName, String message) {
        super("Connection failed for data source '%s': %s".formatted(dataSourceName, message));
        this.dataSourceName = dataSourceName;
    }
    
    public ConnectionException(String dataSourceName, Throwable cause) {
        super("Connection failed for data source '%s'".formatted(dataSourceName), cause);
        this.dataSourceName = dataSourceName;
    }
    
    public String getDataSourceName() {
        return dataSourceName;
    }
}

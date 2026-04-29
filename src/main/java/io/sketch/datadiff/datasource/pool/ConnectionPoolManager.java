package io.sketch.datadiff.datasource.pool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connection pool manager for multiple data sources.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class ConnectionPoolManager implements AutoCloseable {
    
    private final Map<String, DataSource> dataSources = new ConcurrentHashMap<>();
    
    /**
     * Register a data source with a name.
     */
    public void registerDataSource(String name, DataSource dataSource) {
        dataSources.put(name, dataSource);
    }
    
    /**
     * Get a data source by name.
     */
    public DataSource getDataSource(String name) {
        DataSource ds = dataSources.get(name);
        if (ds == null) {
            throw new IllegalArgumentException("Data source not found: " + name);
        }
        return ds;
    }
    
    /**
     * Get a connection from a named data source.
     */
    public Connection getConnection(String name) throws SQLException {
        return getDataSource(name).getConnection();
    }
    
    /**
     * Remove and close a data source.
     */
    public void removeDataSource(String name) {
        DataSource ds = dataSources.remove(name);
        if (ds instanceof AutoCloseable) {
            try {
                ((AutoCloseable) ds).close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close data source: " + name, e);
            }
        }
    }
    
    /**
     * Close all data sources.
     */
    @Override
    public void close() {
        dataSources.forEach((name, ds) -> {
            if (ds instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) ds).close();
                } catch (Exception e) {
                    // Log but continue closing others
                    System.err.println("Failed to close data source: " + name);
                }
            }
        });
        dataSources.clear();
    }
    
    /**
     * Get number of registered data sources.
     */
    public int getDataSourceCount() {
        return dataSources.size();
    }
}

package io.sketch.datadiff.datasource.jdbc;

import io.sketch.datadiff.core.spi.RowMapper;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JDBC query executor for executing SQL queries and mapping results.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class JdbcQueryExecutor {
    
    private final RowMapper rowMapper;
    
    public JdbcQueryExecutor() {
        this.rowMapper = RowMapper.DEFAULT;
    }
    
    public JdbcQueryExecutor(RowMapper rowMapper) {
        this.rowMapper = rowMapper;
    }
    
    /**
     * Execute a query and return all rows.
     */
    public List<Map<String, Object>> executeQuery(
        java.sql.Connection connection,
        String sql,
        Object... params
    ) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        
        try (var stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            
            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(rowMapper.mapRow(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Execute a query that returns a single value.
     */
    public <T> T executeScalar(
        java.sql.Connection connection,
        String sql,
        Class<T> type,
        Object... params
    ) throws SQLException {
        try (var stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            
            try (var rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1, type);
                }
            }
        }
        
        return null;
    }
    
    /**
     * Execute an update/insert/delete statement.
     */
    public int executeUpdate(
        java.sql.Connection connection,
        String sql,
        Object... params
    ) throws SQLException {
        try (var stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            return stmt.executeUpdate();
        }
    }
    
    /**
     * Set prepared statement parameters.
     */
    private void setParameters(java.sql.PreparedStatement stmt, Object[] params) 
        throws SQLException {
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
    }
}

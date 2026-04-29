package io.sketch.datadiff.core.spi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Service Provider Interface for mapping ResultSet rows to data structures.
 */
@FunctionalInterface
public interface RowMapper {
    
    /**
     * Map a single ResultSet row to a Map of column names to values.
     * 
     * @param resultSet the ResultSet positioned at the current row
     * @return Map containing column names as keys and row values
     * @throws SQLException if a database access error occurs
     */
    Map<String, Object> mapRow(ResultSet resultSet) throws SQLException;
    
    /**
     * Default implementation that maps all columns.
     */
    RowMapper DEFAULT = (ResultSet rs) -> {
        java.sql.ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        Map<String, Object> row = new java.util.LinkedHashMap<>(columnCount);
        
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object value = rs.getObject(i);
            row.put(columnName, value);
        }
        
        return row;
    };
}

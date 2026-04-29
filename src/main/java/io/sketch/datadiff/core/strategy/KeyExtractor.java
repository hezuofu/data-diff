package io.sketch.datadiff.core.strategy;

import java.util.Map;
import java.math.BigInteger;

/**
 * Strategy interface for extracting primary key values from row data.
 *
 * @author lanxia39@163.com
 *
 * 
 */
@FunctionalInterface
public interface KeyExtractor {
    
    /**
     * Extract primary key value from row data.
     * 
     * @param rowData row data map
     * @param keyColumns primary key column names
     * @return primary key as BigInteger (for numeric keys)
     */
    BigInteger extractKey(Map<String, Object> rowData, java.util.List<String> keyColumns);
    
    /**
     * Default implementation for single numeric primary key.
     */
    KeyExtractor NUMERIC = (rowData, keyColumns) -> {
        if (keyColumns.size() != 1) {
            throw new IllegalArgumentException("NUMERIC extractor only supports single column keys");
        }
        Object value = rowData.get(keyColumns.get(0));
        if (value == null) {
            throw new IllegalArgumentException("Primary key value cannot be null");
        }
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else if (value instanceof Long) {
            return BigInteger.valueOf((Long) value);
        } else if (value instanceof Integer) {
            return BigInteger.valueOf((Integer) value);
        } else if (value instanceof Short) {
            return BigInteger.valueOf((Short) value);
        } else if (value instanceof String) {
            return new BigInteger((String) value);
        } else {
            throw new IllegalArgumentException("Unsupported key type: " + value.getClass());
        }
    };
}

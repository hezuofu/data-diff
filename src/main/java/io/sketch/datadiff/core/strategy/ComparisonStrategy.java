package io.sketch.datadiff.core.strategy;

import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.model.TableInfo;
import javax.sql.DataSource;

/**
 * Strategy interface for comparison algorithms.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public interface ComparisonStrategy {
    
    /**
     * Compare two tables and return diff results.
     * 
     * @param leftDataSource left table data source
     * @param rightDataSource right table data source
     * @param leftTable left table info
     * @param rightTable right table info
     * @param options comparison options
     * @return diff result
     */
    DiffResult compare(
        DataSource leftDataSource,
        DataSource rightDataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        CompareOptions options
    );
    
    /**
     * Get the strategy name.
     * 
     * @return strategy name
     */
    String getStrategyName();
}

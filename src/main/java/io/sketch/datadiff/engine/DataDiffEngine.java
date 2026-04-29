package io.sketch.datadiff.engine;

import io.sketch.datadiff.exception.DataDiffException;
import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.core.strategy.ComparisonStrategy;
import io.sketch.datadiff.builder.DataDiffBuilder;

import javax.sql.DataSource;

/**
 * Main entry point for data diff operations.
 * Provides a unified interface for comparing tables across databases.
 *
 * @author lanxia39@163.com
 */
public class DataDiffEngine {
    
    private final DataSource leftDataSource;
    private final DataSource rightDataSource;
    private final ComparisonStrategy strategy;
    
    private DataDiffEngine(DataSource leftDataSource, DataSource rightDataSource, ComparisonStrategy strategy) {
        this.leftDataSource = leftDataSource;
        this.rightDataSource = rightDataSource;
        this.strategy = strategy;
    }
    
    /**
     * Compare two tables with default options.
     */
    public DiffResult compare(TableInfo leftTable, TableInfo rightTable) {
        return compare(leftTable, rightTable, CompareOptions.defaults());
    }
    
    /**
     * Compare two tables with specified options.
     */
    public DiffResult compare(TableInfo leftTable, TableInfo rightTable, CompareOptions options) {
        validateTables(leftTable, rightTable);
        return strategy.compare(leftDataSource, rightDataSource, leftTable, rightTable, options);
    }
    
    /**
     * Compare a table with itself (should have no differences).
     */
    public DiffResult compareSelf(TableInfo table) {
        return compare(table, table, CompareOptions.defaults());
    }
    
    /**
     * Get the comparison strategy being used.
     */
    public ComparisonStrategy getStrategy() {
        return strategy;
    }
    
    /**
     * Create a new builder for DataDiffEngine.
     */
    public static DataDiffBuilder builder() {
        return new DataDiffBuilder();
    }
    
    private void validateTables(TableInfo leftTable, TableInfo rightTable) {
        if (!leftTable.primaryKey().equals(rightTable.primaryKey())) {
            throw new DataDiffException(
                "Primary keys must match: left=" + leftTable.primaryKey() + 
                ", right=" + rightTable.primaryKey()
            );
        }
    }
    
    /**
     * Package-private constructor for builder access.
     */
    public static DataDiffEngine create(DataSource leftDs, DataSource rightDs, ComparisonStrategy strategy) {
        return new DataDiffEngine(leftDs, rightDs, strategy);
    }
}

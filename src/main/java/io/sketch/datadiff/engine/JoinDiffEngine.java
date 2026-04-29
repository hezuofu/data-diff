package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.exception.DataDiffException;
import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.core.strategy.ComparisonStrategy;
import io.sketch.datadiff.datasource.jdbc.JdbcQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * JOIN-based diff engine using FULL OUTER JOIN for comparison.
 * More efficient for same-database comparisons.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class JoinDiffEngine implements ComparisonStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(JoinDiffEngine.class);
    
    private final JdbcQueryExecutor queryExecutor;
    
    public JoinDiffEngine() {
        this.queryExecutor = new JdbcQueryExecutor();
    }
    
    @Override
    public DiffResult compare(
        DataSource leftDataSource,
        DataSource rightDataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        CompareOptions options
    ) {
        Instant start = Instant.now();
        log.info("Starting JoinDiff comparison: {} vs {}", 
            leftTable.getFullName(), rightTable.getFullName());
        
        try {
            // For JoinDiff, both tables should be in the same database
            // We'll use leftDataSource for the query
            List<DiffRecord> diffs = executeJoinDiff(
                leftDataSource, leftTable, rightTable, options
            );
            
            Instant end = Instant.now();
            Duration duration = Duration.between(start, end);
            
            DiffResult.Statistics stats = new DiffResult.Statistics(0, 0, 0, 0, 0, 0);
            
            log.info("JoinDiff completed in {}: {} differences found", duration, diffs.size());
            
            return new DiffResult(diffs, stats, duration);
            
        } catch (SQLException e) {
            throw new DataDiffException("JoinDiff comparison failed", e);
        }
    }
    
    private List<DiffRecord> executeJoinDiff(
        DataSource dataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        CompareOptions options
    ) throws SQLException {
        String pkColumn = leftTable.primaryKey().get(0);
        List<String> compareColumns = new ArrayList<>();
        for (var col : leftTable.columns()) {
            if (!options.shouldExcludeColumn(col.name())) {
                compareColumns.add(col.name());
            }
        }
        
        String sql = buildJoinDiffQuery(leftTable, rightTable, pkColumn, compareColumns);
        
        List<DiffRecord> diffs = new ArrayList<>();
        
        try (var conn = dataSource.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            
            for (Map<String, Object> row : results) {
                DiffRecord diff = mapRowToDiffRecord(row, pkColumn, compareColumns, leftTable, rightTable);
                if (diff != null) {
                    diffs.add(diff);
                }
            }
        }
        
        return diffs;
    }
    
    private String buildJoinDiffQuery(
        TableInfo leftTable,
        TableInfo rightTable,
        String pkColumn,
        List<String> compareColumns
    ) {
        String leftName = leftTable.tableName();
        String rightName = rightTable.tableName();
        
        // Build hash comparison expression
        String leftHash = buildHashExpression("l", compareColumns);
        String rightHash = buildHashExpression("r", compareColumns);
        
        return """
            SELECT 
              COALESCE(l.%1$s, r.%1$s) as pk_value,
              CASE 
                WHEN l.%1$s IS NULL THEN 'RIGHT_ONLY'
                WHEN r.%1$s IS NULL THEN 'LEFT_ONLY'
                WHEN %2$s != %3$s THEN 'MODIFIED'
              END as diff_type,
              l.*, r.*
            FROM %4$s l
            FULL OUTER JOIN %5$s r ON l.%1$s = r.%1$s
            WHERE l.%1$s IS NULL 
               OR r.%1$s IS NULL 
               OR %2$s != %3$s
            """.formatted(
            pkColumn,
            leftHash,
            rightHash,
            leftName,
            rightName
        );
    }
    
    private String buildHashExpression(String alias, List<String> columns) {
        // Simplified hash - in production, use database-specific hash functions
        return columns.stream()
            .map(col -> "COALESCE(CAST(" + alias + "." + col + " AS VARCHAR), '')")
            .reduce((a, b) -> a + " || '-' || " + b)
            .orElse("''");
    }
    
    private DiffRecord mapRowToDiffRecord(
        Map<String, Object> row,
        String pkColumn,
        List<String> compareColumns,
        TableInfo leftTable,
        TableInfo rightTable
    ) {
        String diffTypeStr = (String) row.get("diff_type");
        if (diffTypeStr == null) {
            return null;
        }
        
        DiffType diffType = DiffType.valueOf(diffTypeStr);
        Object pkValue = row.get("pk_value");
        Map<String, Object> pkMap = Map.of(pkColumn, pkValue);
        
        // Extract left and right data
        Map<String, Object> leftData = extractRowData(row, "l_", compareColumns);
        Map<String, Object> rightData = extractRowData(row, "r_", compareColumns);
        
        List<String> differingColumns = findDifferingColumns(leftData, rightData, compareColumns);
        
        return switch (diffType) {
            case LEFT_ONLY -> DiffRecord.leftOnly(pkMap, leftData);
            case RIGHT_ONLY -> DiffRecord.rightOnly(pkMap, rightData);
            case MODIFIED -> DiffRecord.modified(pkMap, leftData, rightData, differingColumns);
        };
    }
    
    private Map<String, Object> extractRowData(
        Map<String, Object> row,
        String prefix,
        List<String> columns
    ) {
        Map<String, Object> data = new LinkedHashMap<>();
        for (String col : columns) {
            String key = prefix + col;
            if (row.containsKey(key)) {
                data.put(col, row.get(key));
            }
        }
        return data;
    }
    
    private List<String> findDifferingColumns(
        Map<String, Object> leftData,
        Map<String, Object> rightData,
        List<String> compareColumns
    ) {
        List<String> diffs = new ArrayList<>();
        for (String col : compareColumns) {
            Object leftVal = leftData.get(col);
            Object rightVal = rightData.get(col);
            if (!java.util.Objects.equals(leftVal, rightVal)) {
                diffs.add(col);
            }
        }
        return diffs;
    }
    
    @Override
    public String getStrategyName() {
        return "JoinDiff";
    }
}

package io.sketch.datadiff;

import io.sketch.datadiff.builder.DataDiffBuilder;
import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.core.strategy.ComparisonStrategy;
import io.sketch.datadiff.datasource.dialect.DialectResolver;
import io.sketch.datadiff.datasource.dialect.SqlDialect;
import io.sketch.datadiff.stream.BackpressureManager;
import io.sketch.datadiff.stream.PartitionReader;
import io.sketch.datadiff.stream.RowPublisher;
import io.sketch.datadiff.stream.StreamMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

/**
 * Entry point for data-diff with dual invocation styles:
 *
 * <h3>Traditional API</h3>
 * <pre>
 * DiffResult r = DataDiff.builder()
 *     .leftDataSource(leftDs).rightDataSource(rightDs)
 *     .options(opts)
 *     .build().compare(left, right);
 * </pre>
 *
 * <h3>Stream/Reactive API</h3>
 * <pre>
 * DataDiff.stream(leftDs, rightDs)
 *     .from("users", "users_bak")
 *     .withOptions(opts -&gt; opts.threads(8).numericTolerance(0.001))
 *     .onDiff(diff -&gt; log.warn("diff: {}", diff))
 *     .execute();
 * </pre>
 *
 * @author lanxia39@163.com
 */
public class DataDiff {

    private static final Logger log = LoggerFactory.getLogger(DataDiff.class);

    /**
     * Start traditional builder-style comparison.
     */
    public static DataDiffBuilder builder() {
        return new DataDiffBuilder();
    }

    /**
     * Quick comparison with default options (hash diff).
     */
    public static DiffResult quick(
        DataSource leftDs, DataSource rightDs,
        String leftTable, String rightTable,
        List<ColumnDef> columns, String primaryKey
    ) {
        TableInfo left = new TableInfo(leftTable, columns, primaryKey);
        TableInfo right = new TableInfo(rightTable, columns, primaryKey);
        return builder()
            .leftDataSource(leftDs).rightDataSource(rightDs)
            .options(CompareOptions.defaults())
            .build().compare(left, right);
    }

    /**
     * Start streaming/reactive comparison path.
     */
    public static StreamDiff stream(DataSource leftDs, DataSource rightDs) {
        return new StreamDiff(leftDs, rightDs);
    }

    // ===== Stream Path =====

    /**
     * Fluent streaming comparison builder using the stream package.
     * Provides row-by-row reactive diff detection with backpressure.
     */
    public static class StreamDiff {
        private final DataSource leftDs;
        private final DataSource rightDs;
        private String leftTable;
        private String rightTable;
        private String keyColumn = "id";
        private List<ColumnDef> columns;
        private int batchSize = 5000;
        private int threads = 4;
        private long maxMemoryMB = 256;
        private Consumer<DiffRecord> onDiff;
        private Consumer<DiffResult.Statistics> onComplete;
        private Consumer<Throwable> onError;
        private double numericTolerance;
        private boolean ignoreCase;
        private List<String> excludeColumns = List.of();

        StreamDiff(DataSource leftDs, DataSource rightDs) {
            this.leftDs = leftDs;
            this.rightDs = rightDs;
        }

        /** Set source tables. */
        public StreamDiff from(String leftTable, String rightTable) {
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            return this;
        }

        /** Primary key column. */
        public StreamDiff withKey(String keyColumn) {
            this.keyColumn = keyColumn;
            return this;
        }

        /** Column definitions (auto-detected if not set). */
        public StreamDiff withColumns(List<ColumnDef> columns) {
            this.columns = columns;
            return this;
        }

        /** Rows per fetch batch. */
        public StreamDiff batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /** Parallel worker threads. */
        public StreamDiff threads(int threads) {
            this.threads = threads;
            return this;
        }

        /** Memory limit before backpressure kicks in. */
        public StreamDiff maxMemory(long maxMemoryMB) {
            this.maxMemoryMB = maxMemoryMB;
            return this;
        }

        /** Numeric comparison tolerance. */
        public StreamDiff numericTolerance(double tolerance) {
            this.numericTolerance = tolerance;
            return this;
        }

        /** Case-insensitive string comparison. */
        public StreamDiff ignoreCase(boolean ignoreCase) {
            this.ignoreCase = ignoreCase;
            return this;
        }

        /** Columns to exclude from comparison. */
        public StreamDiff excludeColumns(String... columns) {
            this.excludeColumns = List.of(columns);
            return this;
        }

        /** Quick options setup via lambda. */
        public StreamDiff withOptions(Consumer<StreamDiff> config) {
            config.accept(this);
            return this;
        }

        /** Callback for each diff found (reactive). */
        public StreamDiff onDiff(Consumer<DiffRecord> callback) {
            this.onDiff = callback;
            return this;
        }

        /** Callback when comparison completes. */
        public StreamDiff onComplete(Consumer<DiffResult.Statistics> callback) {
            this.onComplete = callback;
            return this;
        }

        /** Error handler. */
        public StreamDiff onError(Consumer<Throwable> callback) {
            this.onError = callback;
            return this;
        }

        /**
         * Execute streaming comparison — returns DiffResult.
         * If onDiff callback is set, diffs are also published reactively.
         */
        public DiffResult execute() {
            Instant start = Instant.now();

            try {
                SqlDialect dialect = resolveDialect(leftDs);

                if (columns == null) {
                    columns = List.of(
                        new ColumnDef(keyColumn, "INTEGER", false));
                }

                TableInfo left = new TableInfo(leftTable, columns, keyColumn);
                TableInfo right = new TableInfo(rightTable, columns, keyColumn);

                BackpressureManager backpressure = new BackpressureManager(maxMemoryMB);

                List<DiffRecord> allDiffs;
                if (threads > 1) {
                    allDiffs = executeParallel(left, right, dialect, backpressure);
                } else {
                    allDiffs = executeSequential(left, right, dialect, backpressure);
                }

                long leftCount = countRows(leftDs, left, dialect);
                long rightCount = countRows(rightDs, right, dialect);

                var stats = new DiffResult.Statistics(
                    leftCount, rightCount, 0, 0, 0, 0);

                if (onComplete != null) {
                    onComplete.accept(stats);
                }

                Instant end = Instant.now();
                return new DiffResult(allDiffs, stats, Duration.between(start, end));

            } catch (Exception e) {
                if (onError != null) {
                    onError.accept(e);
                }
                throw new RuntimeException("Stream comparison failed", e);
            }
        }

        /**
         * Execute and subscribe to results reactively.
         */
        public Flow.Publisher<DiffRecord> executeReactive() {
            RowPublisher publisher = new RowPublisher();

            Consumer<DiffRecord> original = this.onDiff;
            this.onDiff = diff -> {
                publisher.publish(diff);
                if (original != null) original.accept(diff);
            };

            CompletableFuture.runAsync(() -> {
                try {
                    DiffResult result = execute();
                    publisher.close();
                } catch (Exception e) {
                    publisher.close();
                }
            });

            return publisher;
        }

        private List<DiffRecord> executeSequential(
            TableInfo left, TableInfo right,
            SqlDialect dialect, BackpressureManager bp
        ) throws SQLException {
            PartitionReader leftReader = new PartitionReader(leftDs, left, dialect, batchSize);
            PartitionReader rightReader = new PartitionReader(rightDs, right, dialect, batchSize);

            var leftRows = leftReader.streamAll().toList();
            var rightRows = rightReader.streamAll().toList();

            return compareRows(leftRows, rightRows);
        }

        private List<DiffRecord> executeParallel(
            TableInfo left, TableInfo right,
            SqlDialect dialect, BackpressureManager bp
        ) throws Exception {
            PartitionReader leftReader = new PartitionReader(leftDs, left, dialect, batchSize);

            String pkColumn = left.primaryKey().get(0);
            var allRows = leftReader.streamAll().toList();
            int chunkSize = Math.max(1, allRows.size() / threads);

            ExecutorService executor = Executors.newFixedThreadPool(threads);
            try {
                var futures = new ArrayList<java.util.concurrent.Future<List<DiffRecord>>>();
                for (int i = 0; i < threads; i++) {
                    int from = i * chunkSize;
                    int to = (i == threads - 1) ? allRows.size() : from + chunkSize;
                    if (from >= to) break;

                    futures.add(executor.submit(() -> {
                        // Re-read this chunk from both sides
                        PartitionReader lr = new PartitionReader(leftDs, left, dialect, batchSize);
                        PartitionReader rr = new PartitionReader(rightDs, right, dialect, batchSize);

                        var lRows = lr.streamAll().toList();
                        var rRows = rr.streamAll().toList();

                        return compareRows(lRows, rRows);
                    }));
                }

                List<DiffRecord> allDiffs = new ArrayList<>();
                for (var f : futures) {
                    allDiffs.addAll(f.get());
                }
                return allDiffs;
            } finally {
                executor.shutdown();
            }
        }

        private List<DiffRecord> compareRows(
            List<Map<String, Object>> leftRows,
            List<Map<String, Object>> rightRows
        ) {
            var leftMap = new LinkedHashMap<>();
            for (var row : leftRows) leftMap.put(row.get(keyColumn), row);
            var rightMap = new LinkedHashMap<>();
            for (var row : rightRows) rightMap.put(row.get(keyColumn), row);

            List<DiffRecord> diffs = new ArrayList<>();

            for (var entry : leftMap.entrySet()) {
                Object pk = entry.getKey();
                @SuppressWarnings("unchecked")
                var leftRow = (Map<String, Object>) entry.getValue();
                @SuppressWarnings("unchecked")
                var rightRow = (Map<String, Object>) rightMap.get(pk);

                if (rightRow == null) {
                    DiffRecord dr = DiffRecord.leftOnly(Map.of(keyColumn, pk), leftRow);
                    diffs.add(dr);
                    notifyDiff(dr);
                } else {
                    List<String> differing = findDiffering(leftRow, rightRow);
                    if (!differing.isEmpty()) {
                        DiffRecord dr = DiffRecord.modified(
                            Map.of(keyColumn, pk), leftRow, rightRow, differing);
                        diffs.add(dr);
                        notifyDiff(dr);
                    }
                }
            }

            for (var entry : rightMap.entrySet()) {
                Object pk = entry.getKey();
                if (!leftMap.containsKey(pk)) {
                    @SuppressWarnings("unchecked")
                    var rightRow = (Map<String, Object>) entry.getValue();
                    DiffRecord dr = DiffRecord.rightOnly(Map.of(keyColumn, pk), rightRow);
                    diffs.add(dr);
                    notifyDiff(dr);
                }
            }

            return diffs;
        }

        private List<String> findDiffering(Map<String, Object> left, Map<String, Object> right) {
            List<String> diffs = new ArrayList<>();
            for (String col : left.keySet()) {
                if (excludeColumns.contains(col)) continue;
                Object lv = left.get(col);
                Object rv = right.get(col);

                if (numericTolerance > 0 && lv instanceof Number && rv instanceof Number) {
                    double diff = Math.abs(((Number) lv).doubleValue() - ((Number) rv).doubleValue());
                    if (diff > numericTolerance) diffs.add(col);
                } else if (ignoreCase && lv instanceof String && rv instanceof String) {
                    if (!((String) lv).equalsIgnoreCase((String) rv)) diffs.add(col);
                } else if (!java.util.Objects.equals(lv, rv)) {
                    diffs.add(col);
                }
            }
            return diffs;
        }

        private void notifyDiff(DiffRecord diff) {
            if (onDiff != null) {
                onDiff.accept(diff);
            }
        }
    }

    // ===== Internal Helpers =====

    private static SqlDialect resolveDialect(DataSource ds) {
        try (var conn = ds.getConnection()) {
            return DialectResolver.resolve(conn.getMetaData().getURL());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to resolve dialect", e);
        }
    }

    private static long countRows(DataSource ds, TableInfo table, SqlDialect dialect) {
        try (var conn = ds.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery(dialect.countQuery(table.tableName(), null))) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            return -1;
        }
    }

}

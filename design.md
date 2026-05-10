# Data Diff Java — Architecture & Design

## 1. Overview

Data Diff Java is a database table comparison framework with three independent strategies and two invocation styles, targeting Python data-diff parity on the JVM.

### 1.1 Design Goals

- **Pluggable strategies**: Route by scenario automatically — checksum-bisect, join, or streaming
- **Memory safe**: Streaming reads + backpressure — handles TB-scale tables without OOM
- **Extensible**: SPI architecture for custom datasources, hash algorithms, and output formats
- **Type safe**: Java 17 Records + immutable objects + Builder validation at compile time
- **Dual APIs**: Traditional Builder API for server workloads, Lambda Stream API for scripting

### 1.2 Feature Matrix

| Feature | Detail |
|---|---|
| Three strategies | HashDiff / JoinDiff / StreamDiff |
| Dual invocation | Builder API (traditional) + Stream API (lambda) |
| Reactive output | `onDiff` callback + `Flow.Publisher` subscription |
| Streaming | PartitionReader batch loading + BackpressureManager |
| Parallelism | Segment-level / chunk-level, configurable threads |
| Databases | MySQL / PostgreSQL / Snowflake / H2 / MariaDB |
| SPI | ChecksumCalculator / DataSourceProvider / ResultFormatter |
| Config | Typesafe Config (HOCON) with env var substitution and file merging |

## 2. Architecture

### 2.1 Layered Architecture

```
Entry Layer
├── DataDiff.stream()             Lambda-style entry point
├── DataDiff.builder()            Builder-style entry point
├── DataDiffBuilder               Builder implementation
└── DataDiffApplication           CLI entry point (config-file driven)

Engine Layer
├── DataDiffEngine                Unified facade, strategy routing
├── ComparisonStrategy            Strategy interface
│   ├── HashDiffEngine            Checksum + recursive bisect
│   ├── JoinDiffEngine            FULL OUTER JOIN
│   └── StreamComparator          Chunked row-by-row
├── RecursiveBisector             Binary search for differing rows
└── SegmentSplitter               Key-range segmenter

Data Access Layer
├── datasource/dialect/           SqlDialect → MySQL, PostgreSQL, Snowflake, H2
├── datasource/jdbc/              JdbcQueryExecutor, JdbcDataSourceProvider
├── datasource/pool/              HikariCPProvider
├── stream/                       PartitionReader / BackpressureManager / RowPublisher
└── hash/                         Md5Checksum / Crc32Checksum / CompositeChecksum

Model Layer
├── core/model/                   TableInfo, Segment, DiffRecord, DiffResult, CompareOptions, ...
├── core/spi/                     ChecksumCalculator, DataSourceProvider, ResultFormatter, RowMapper
└── core/strategy/                ComparisonStrategy, SplitStrategy, OrderingStrategy, KeyExtractor

Extension Layer
├── comparator/                   ColumnComparator, RowComparator, NumericToleranceComparator, ...
├── output/                       JsonFormatter, CsvFormatter, TableFormatter, StatsFormatter
├── config/                       AppConfig, ConfigLoader
└── exception/                    DataDiffException, ConnectionException, ChecksumMismatchException
```

### 2.2 Dependency Graph

```
config → core.model
   ↓
engine → core.model, core.spi, core.strategy
   ↓         ↓              ↓              ↓
datasource  hash        comparator     output
   ↓         ↓              ↓              ↓
database   CRC32/MD5     Row/Column     JSON/CSV
```

## 3. Strategy Design

### 3.1 StrategyType Enum

```java
public enum StrategyType {
    HASH,     // HashDiff: checksum + recursive bisect
    JOIN,     // JoinDiff: FULL OUTER JOIN
    STREAM,   // StreamDiff: chunked row-by-row
    AUTO      // defaults to HASH (cross-DB safe)
}
```

`DataDiffBuilder.build()` instantiates the correct strategy implementation based on `StrategyType`.

### 3.2 HashDiff Algorithm

**Flow**:
```
1. Determine key range [min, max]
2. Split into segments (bisectionFactor)
   ↓
3. For each segment (parallel), compute SQL-level checksums for left and right
4. Compare checksums
   ├─ Match → skip segment (no diffs)
   └─ Mismatch → RecursiveBisector
      ├─ Segment count > bisectionThreshold → bisect further
      └─ Segment count ≤ bisectionThreshold → extract rows, compare
```

**Key classes**: `HashDiffEngine` → `SegmentSplitter` → `RecursiveBisector`

**Best for**: Large tables with few differences, cross-database comparisons.

### 3.3 JoinDiff Algorithm

**Flow**:
```
1. Build FULL OUTER JOIN SQL
2. Execute single query
   ├─ l.key IS NULL → RIGHT_ONLY
   ├─ r.key IS NULL → LEFT_ONLY
   └─ hash(l.*) ≠ hash(r.*) → MODIFIED
```

**Limitation**: Both tables must reside in the same database.

**Best for**: Same-database small/medium tables. Fastest — single query.

### 3.4 StreamDiff Algorithm

**Flow**:
```
1. Determine key range
2. Walk through range in chunks (bisectionFactor)
3. For each chunk:
   ├─ Fetch rows from left and right tables (via PartitionReader batches)
   ├─ Compare rows directly (no checksum pre-filter)
   └─ Optional parallel execution at chunk level
4. Optional real-time callbacks via onDiff / Flow.Publisher
```

**Best for**: Many diffs expected, medium-scale tables, real-time callback needs, memory-constrained environments.

### 3.5 Strategy Comparison Matrix

| Dimension | HashDiff | JoinDiff | StreamDiff |
|---|---|---|---|
| Cross-DB | Yes | No | Yes |
| Checksum pre-filter | Yes | No (SQL handles) | No |
| Recursive bisect | Yes | No | No |
| Parallelism | Segment-level | None (single query) | Chunk-level |
| Memory control | Low (streaming) | Low (SQL handles) | Controllable (BackpressureManager) |
| Optimal scenario | Large table, few diffs | Same-DB, small/medium | Many diffs, callbacks needed |

## 4. Core Data Model

```java
public record TableInfo(String tableName, String schemaName,
    List<ColumnDef> columns, List<String> primaryKey, List<String> indexes) {}

public record ColumnDef(String name, String dataType, boolean nullable) {
    public boolean isNumeric();     // INT, BIGINT, DECIMAL, FLOAT, DOUBLE, REAL, ...
    public boolean isString();      // VARCHAR, CHAR, TEXT, ...
    public boolean isDateTime();    // DATE, TIME, DATETIME, TIMESTAMP, ...
}

public record Segment(BigInteger rangeStart, BigInteger rangeEnd,
    long count, BigInteger checksum, int depth) {
    public Segment[] bisect();      // binary split
    public BigInteger rangeSize();  // range width
}

public record DiffRecord(Map<String, Object> primaryKey,
    Map<String, Object> leftData, Map<String, Object> rightData,
    DiffType diffType, List<String> differingColumns) {
    public static DiffRecord leftOnly(...);
    public static DiffRecord rightOnly(...);
    public static DiffRecord modified(...);
}

public enum DiffType { LEFT_ONLY, RIGHT_ONLY, MODIFIED }

public class DiffResult {
    public List<DiffRecord> getDiffRecords();
    public Map<DiffType, Long> getDiffCounts();
    public boolean hasDifferences();
    public long getDiffCount();
    public Statistics getStatistics();
}

public class CompareOptions {
    public enum StrategyType { HASH, JOIN, STREAM, AUTO }
    public static Builder builder() { ... }
    // Builder validates: bisectionFactor ≥ 1, threads ≥ 1, numericTolerance ≥ 0
}
```

## 5. SPI Extension Points

### 5.1 Interfaces

```java
public interface ChecksumCalculator {
    BigInteger checksum(Object value);
    default BigInteger checksum(List<Object> values) { ... }
    String getAlgorithmName();
}

public interface DataSourceProvider {
    boolean supports(String jdbcUrl);
    Connection getConnection(String url, Properties props);
    SqlDialect getDialect();
}

public interface ResultFormatter {
    void format(DiffResult result, OutputStream outputStream);
    String getFormatName();
}
```

### 5.2 Registration Methods

- **Programmatic**: `HashFunctionRegistry.register("sha256", new Sha256Checksum())`
- **SPI file**: `META-INF/services/io.sketch.datadiff.core.spi.ChecksumCalculator`

## 6. Invocation Style Design

### 6.1 Builder API (Traditional)

```java
DataDiff.builder()
    .leftDataSource(leftDs).rightDataSource(rightDs)
    .options(CompareOptions.builder().strategy(HASH).threads(8).build())
    .build().compare(leftTable, rightTable);
```

For: Server-side integration, batch jobs.

### 6.2 Stream API (Lambda)

```java
DataDiff.stream(leftDs, rightDs)
    .from("users", "users_bak")
    .withOptions(opts -> opts.threads(8).batchSize(5000))
    .onDiff(diff -> process(diff))
    .execute();
```

For: Scripting, exploratory analysis, real-time callback needs.

### 6.3 Reactive API

```java
Flow.Publisher<DiffRecord> pub = DataDiff.stream(leftDs, rightDs)
    .from("users", "users_bak")
    .executeReactive();
pub.subscribe(mySubscriber);
```

For: Event-driven architectures, stream processing pipelines.

## 7. Database Dialect System

```java
public interface SqlDialect {
    String quoteIdentifier(String name);
    String hashExpression(List<String> columns);
    String checksumQuery(String table, List<String> cols, String key, long start, long end);
    String countQuery(String table, String where);
    String minMaxQuery(String table, String column);
    String getDriverClassName();
    String getDialectName();
    default boolean supportsFullOuterJoin() { return true; }
}
```

Auto-detection: `DialectResolver.resolve(jdbcUrl)` matches URL prefix.

### Implementations

| Dialect | Identifier Quote | Hash Function |
|---|---|---|
| MySqlDialect | backtick `` `col` `` | `BIT_XOR(CRC32(CONCAT_WS(...)))` |
| PostgreSqlDialect | double-quote `"col"` | `hashtext(...)::bigint` |
| H2Dialect | double-quote `"col"` | `HASH('MD5', ...)` |
| SnowflakeDialect | double-quote `"col"` | `HASH(...)` |

## 8. Streaming & Memory Safety

### 8.1 PartitionReader

Batch-loads rows using LIMIT/OFFSET to avoid loading entire tables:

```java
PartitionReader reader = new PartitionReader(dataSource, table, dialect, 5000);
Stream<Map<String, Object>> rows = reader.streamAll();  // lazy stream
```

### 8.2 BackpressureManager

Threshold-based memory backpressure:

```java
BackpressureManager bp = new BackpressureManager(256); // 256MB limit
if (bp.recordMemoryIncrease(rowSize)) {
    bp.waitForBackpressureRelief();  // block until memory freed
}
```

### 8.3 RowPublisher

Reactive publishing via `java.util.concurrent.Flow.Publisher`:

```java
RowPublisher publisher = new RowPublisher();
publisher.subscribe(subscriber);
publisher.publish(diffRecord);      // push individual diffs
publisher.publishAll(diffs);        // push batch
```

## 9. Parallel Processing

HashDiff parallelizes at the segment level — each segment's checksum is computed in parallel via a fixed thread pool.

StreamDiff parallelizes at the chunk level — each chunk's row fetch and comparison runs in a thread pool.

Both use `CompareOptions.threads()` to configure pool size.

## 10. Configuration System

Uses Typesafe Config (HOCON format) with support for:

```hocon
# Environment variables
password: ${DB_PASSWORD:"default"}

# Path references
right.url: ${left.url}

# File inclusion
include "base.conf"

# Arrays and objects
excludeColumns: ["updated_at", "created_at"]
customComparators { status: "string", amount: "numeric" }
```

## 11. Error Handling

```
DataDiffException (RuntimeException)
├── ConnectionException           // database connection failure
├── ChecksumMismatchException     // checksum computation error
└── UnsupportedKeyTypeException   // unsupported primary key type
```

All exceptions are unchecked with chained cause support.

## 12. Test Strategy

| Layer | Count | Scope |
|---|---|---|
| Model unit tests | ~30 | Record validation, builder constraints |
| Comparator tests | ~20 | Numeric tolerance, string normalization, row diff |
| Hash/Dialect tests | ~25 | Checksum consistency, SQL generation |
| H2 integration tests | ~40 | End-to-end engine, RecursiveBisector, all three strategies |
| TestContainers | 2 | PostgreSQL/MySQL real environment (requires Docker) |
| **Total** | **143** | 0 failures |

## 13. Summary

Data Diff Java achieves high-performance, extensible database comparison through:

1. **Three-strategy engine** matching different scenarios with automatic routing
2. **Dual invocation styles** serving both server integration and exploratory scripting
3. **SPI architecture** enabling pluggable extensions
4. **Streaming + backpressure** supporting terabyte-scale datasets
5. **Type-safe configuration** catching errors at startup, not at runtime
6. **Functional design** with Records, immutability, and composable lambdas

---

**Author**: lanxia39@163.com
**Version**: 1.0.0
**Java**: 17+

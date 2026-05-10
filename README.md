# Data Diff Java

A Java 17 database table diff framework — three comparison strategies, dual invocation styles, memory-safe streaming for terabyte-scale tables.

## Features

- **Three strategies**: HashDiff (checksum + recursive bisect), JoinDiff (FULL OUTER JOIN), StreamDiff (chunked row-by-row)
- **Dual APIs**: Traditional Builder API for server integration, `DataDiff.stream()` Lambda style for scripting and exploratory analysis
- **Reactive callbacks**: `onDiff()` pushes diffs in real time, `executeReactive()` exposes a `Flow.Publisher` for subscriber patterns
- **Memory safe**: PartitionReader batch loading + BackpressureManager throttling — no OOM on TB-scale tables
- **SPI pluggable**: custom datasources, hash algorithms, and output formats via interfaces with auto-discovery
- **Multi-database**: MySQL, PostgreSQL, Snowflake, H2 with automatic dialect detection
- **Advanced comparison**: numeric tolerance, string normalization, column exclusion, case-insensitive matching
- **Modern Java**: Records, sealed/switch expressions, Stream API, immutable objects throughout

## Quick Start

### Option 1: Config File

Create `my-config.conf`:

```hocon
left {
  url: "jdbc:mysql://localhost:3306/db1"
  username: "root"
  password: ${DB_PASSWORD}           # env var support
  table: "users"
  primaryKey: ["id"]
  excludeColumns: ["updated_at", "created_at"]
}

right {
  url: "jdbc:mysql://localhost:3306/db2"
  username: "root"
  password: ${DB_PASSWORD}
  table: "users"
}

comparison {
  algorithm: "hashdiff"              # hashdiff | joindiff | streamdiff
  bisectionFactor: 32
  bisectionThreshold: 16384
  threads: 4
  numericTolerance: 0.001
}

output {
  format: "json"                     # json | csv | table | stats
  outputFile: "diff-result.json"
  showStats: true
}
```

Run:

```bash
java -jar data-diff.jar --config my-config.conf
```

### Option 2: Builder API (Traditional)

```java
DataSource leftDs = HikariCPProvider.createDataSource("jdbc:mysql://host/db1", props);
DataSource rightDs = HikariCPProvider.createDataSource("jdbc:mysql://host/db2", props);

List<ColumnDef> columns = List.of(
    new ColumnDef("id", "INTEGER", false),
    new ColumnDef("name", "VARCHAR", true),
    new ColumnDef("amount", "DECIMAL", true)
);

TableInfo left = new TableInfo("users", columns, "id");
TableInfo right = new TableInfo("users_bak", columns, "id");

CompareOptions opts = CompareOptions.builder()
    .strategy(CompareOptions.StrategyType.HASH)
    .bisectionFactor(64)
    .threads(8)
    .numericTolerance(0.001)
    .excludeColumns("updated_at")
    .build();

DiffResult result = DataDiff.builder()
    .leftDataSource(leftDs).rightDataSource(rightDs)
    .options(opts)
    .build().compare(left, right);

if (result.hasDifferences()) {
    System.out.println("Found " + result.getDiffCount() + " differences");
    result.getDiffCounts().forEach((type, count) ->
        System.out.println("  " + type + ": " + count));
    new JsonFormatter().format(result, System.out);
}
```

### Option 3: Stream API (Lambda style)

```java
// Callback style — handle each diff as it's found
DiffResult result = DataDiff.stream(leftDs, rightDs)
    .from("users", "users_bak")
    .withKey("id")
    .withOptions(opts -> opts
        .threads(8)
        .batchSize(5000)
        .numericTolerance(0.001))
    .onDiff(diff -> log.warn("Diff: pk={}, type={}, cols={}",
        diff.getPrimaryKeyString(), diff.diffType(), diff.differingColumns()))
    .onComplete(stats -> log.info("Comparison complete"))
    .onError(err -> log.error("Comparison failed", err))
    .execute();

// Reactive style — Flow.Publisher subscription
Flow.Publisher<DiffRecord> publisher = DataDiff.stream(leftDs, rightDs)
    .from("users", "users_bak")
    .executeReactive();

publisher.subscribe(new Flow.Subscriber<>() {
    public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
    public void onNext(DiffRecord diff) { handle(diff); }
    public void onError(Throwable t) { log.error("fail", t); }
    public void onComplete() { log.info("done"); }
});
```

## Strategy Selection

| Strategy | Method | Cross-DB | Best For |
|---|---|---|---|
| **HashDiff** | Checksum + recursive bisect | Yes | Large tables, few diffs, cross-database |
| **JoinDiff** | FULL OUTER JOIN | No | Same-database, small/medium tables |
| **StreamDiff** | Chunked row-by-row | Yes | Many diffs expected, real-time callbacks needed |

### Strategy Routing

```java
// DataDiffBuilder automatically selects engine based on StrategyType
var strategy = switch (opts.getStrategy()) {
    case HASH   -> new HashDiffEngine();     // checksum + recursive bisect
    case JOIN   -> new JoinDiffEngine();     // single FULL OUTER JOIN query
    case STREAM -> new StreamComparator();   // chunked row-by-row comparison
    case AUTO   -> new HashDiffEngine();     // safe cross-database default
};
```

## Architecture

```
Entry Points
├── DataDiff.stream()             Lambda API → uses stream package
├── DataDiff.builder()            Builder API → DataDiffBuilder
└── DataDiffApplication           CLI entry point (config-file driven)

Engine Layer
├── DataDiffEngine                Unified entry point, strategy routing
├── ComparisonStrategy            Strategy interface
│   ├── HashDiffEngine            Checksum pre-filter + recursive bisect
│   ├── JoinDiffEngine            FULL OUTER JOIN (single query)
│   └── StreamComparator          Chunked row-by-row comparison
├── RecursiveBisector             Binary search for differing rows
└── SegmentSplitter               Key-range segmentation

Data Access Layer
├── datasource/dialect/           SqlDialect → MySQL, PostgreSQL, Snowflake, H2
├── datasource/jdbc/              JdbcQueryExecutor, JdbcDataSourceProvider
├── datasource/pool/              HikariCPProvider (HikariCP connection pool)
├── stream/                       PartitionReader → BackpressureManager → RowPublisher
└── hash/                         ChecksumCalculator → Md5, Crc32, Composite

Model Layer
├── core/model/                   TableInfo, Segment, DiffRecord, DiffResult, CompareOptions, ...
├── core/spi/                     ChecksumCalculator, DataSourceProvider, ResultFormatter, RowMapper
└── core/strategy/                ComparisonStrategy, SplitStrategy, OrderingStrategy

Extension Layer
├── comparator/                   ColumnComparator, RowComparator, CompositeComparator
├── output/                       JsonFormatter, CsvFormatter, TableFormatter
├── config/                       AppConfig, ConfigLoader (Typesafe Config)
└── exception/                    DataDiffException, ConnectionException
```

## Database Support

| Database | Dialect | Hashing | Connection Pool |
|---|---|---|---|
| MySQL 8.0+ | MySqlDialect | BIT_XOR(CRC32) | HikariCP |
| PostgreSQL 12+ | PostgreSqlDialect | hashtext() | HikariCP |
| Snowflake | SnowflakeDialect | HASH() | HikariCP |
| H2 | H2Dialect | HASH('MD5') | HikariCP |
| MariaDB | MySqlDialect (reuse) | same as MySQL | HikariCP |

## Extension Guide

### Custom Hash Algorithm

```java
public class Sha256Checksum implements ChecksumCalculator {
    public BigInteger checksum(Object value) {
        var md = MessageDigest.getInstance("SHA-256");
        return new BigInteger(1, md.digest(value.toString().getBytes(UTF_8)));
    }
    public String getAlgorithmName() { return "SHA256"; }
}

HashFunctionRegistry.register("sha256", new Sha256Checksum());
```

### Custom Database Dialect

```java
public class OracleDialect extends AbstractSqlDialect {
    public String hashExpression(List<String> columns) {
        return "ORA_HASH(" + columns.stream()
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(" || '|' || ")) + ")";
    }
    public String getDriverClassName() { return "oracle.jdbc.OracleDriver"; }
    public String getDialectName() { return "Oracle"; }
}

DialectResolver.registerDialect("jdbc:oracle:", new OracleDialect());
```

### Custom Output Format

```java
public class HtmlFormatter implements ResultFormatter {
    public void format(DiffResult result, OutputStream out) {
        var w = new PrintWriter(out);
        w.println("<table>");
        for (var d : result.getDiffRecords())
            w.printf("<tr><td>%s</td><td>%s</td></tr>%n",
                d.getPrimaryKeyString(), d.diffType());
        w.println("</table>");
        w.flush();
    }
    public String getFormatName() { return "html"; }
}
```

## Build & Test

```bash
# Requires Java 17+, Maven 3.6+

mvn clean compile                    # compile
mvn clean package -DskipTests        # package executable JAR
mvn test                             # run 143 tests
mvn clean install                    # install to local Maven repo
```

### Maven Dependency

```xml
<dependency>
    <groupId>io.sketch.datadiff</groupId>
    <artifactId>data-diff-java</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Tuning Guide

| Scenario | Recommendation |
|---|---|
| TB-scale table, few diffs | `strategy: HASH`, `bisectionFactor: 64`, `threads: 16` |
| Many diffs, medium scale | `strategy: STREAM`, `bisectionFactor: 10000`, `threads: 8` |
| Same-database quick check | `strategy: JOIN` (no extra tuning needed) |
| Float column comparison | `numericTolerance: 0.001` |
| Ignore timestamps | `excludeColumns: ["created_at", "updated_at"]` |
| Case-insensitive | `caseInsensitive: true` |
| Memory-constrained | Stream path + `maxMemory(128)` + `batchSize(1000)` |
| Production | `threads` = CPU cores, `bisectionFactor` = `√rowCount` |

## FAQ

**Which strategy should I use?**

| Scenario | Recommendation |
|---|---|
| Cross-database | HashDiff or StreamDiff |
| Same DB, small/medium table | JoinDiff (fastest) |
| Large table, few diffs expected | HashDiff (checksums skip matched segments) |
| Many diffs expected | StreamDiff (no checksum overhead) |
| Not sure | AUTO (defaults to HashDiff) |

**Composite primary keys?** Supported — `TableInfo` primaryKey accepts `List<String>`.

**UUID primary keys?** Supported as long as the type implements `Comparable`.

**Will large tables cause OOM?** No. `PartitionReader` loads rows in configurable batches. `BackpressureManager` throttles when memory exceeds the configured threshold. Both HashDiff and StreamDiff use streaming reads.

**How do I customize comparison logic?** Implement `ColumnComparator` and register it via `CompositeComparator.registerComparator(colName, cmp)`. For Stream path, you can also filter in the `onDiff` callback.

## Author

lanxia39@163.com

## License

Apache License 2.0

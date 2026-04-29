# Data Diff Java

A modern Java 21 implementation of data-diff for comparing tables within or across SQL databases.

## Features

- **Dual Algorithm Support**: HashDiff (cross-database) and JoinDiff (same-database)
- **Virtual Threads**: Java 21 virtual threads for efficient I/O operations
- **SPI Architecture**: Extensible via Service Provider Interface
- **Functional Programming**: Records, functional interfaces, stream-friendly APIs
- **Multiple Database Support**: MySQL, PostgreSQL, Snowflake with dialect system
- **Advanced Comparison**: Numeric tolerance, case-insensitive comparison, column exclusion
- **Multiple Output Formats**: JSON, Table, CSV, HTML reports
- **Caching**: Caffeine-based checksum caching for performance
- **Zero Core Dependencies**: Core algorithms have no external dependencies

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>com.datadiff</groupId>
    <artifactId>data-diff-java</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Basic Usage

```java

import io.sketch.datadiff.core.model.ColumnDef;
import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.engine.DataDiffEngine;

// Create data sources (using HikariCP)
Properties leftProps = new Properties();
leftProps.

        setProperty("user","root");
leftProps.

        setProperty("password","password");

        DataSource leftDs = HikariCPProvider.createDataSource(
                "jdbc:mysql://localhost:3306/db1", leftProps
        );

        DataSource rightDs = HikariCPProvider.createDataSource(
                "jdbc:mysql://localhost:3306/db2", rightProps
        );

        // Build engine
        DataDiffEngine engine = DataDiffEngine.builder()
                .leftDataSource(leftDs)
                .rightDataSource(rightDs)
                .options(CompareOptions.builder()
                        .algorithm(CompareOptions.Algorithm.HASHDIFF)
                        .segmentSize(50000)
                        .parallelism(8)
                        .build())
                .build();

        // Define tables
        TableInfo leftTable = new TableInfo(
                "users",
                List.of(
                        new ColumnDef("id", "BIGINT", false),
                        new ColumnDef("name", "VARCHAR", false),
                        new ColumnDef("email", "VARCHAR", true)
                ),
                List.of("id")
        );

        // Compare
        DiffResult result = engine.compare(leftTable, leftTable);

// Output results
if(result.

        hasDifferences()){
        System.out.

        println("Found "+result.getDiffCount() +" differences");
        new

        TableFormatter().

        format(result, System.out);
}else{
        System.out.

        println("Tables are identical");
}
```

### Advanced Configuration

```java
CompareOptions options = CompareOptions.builder()
    .algorithm(CompareOptions.Algorithm.JOINDIFF)  // or HASHDIFF
    .segmentSize(100000)                            // rows per segment
    .maxBisectionDepth(32)                          // max recursion depth
    .parallelism(16)                                // virtual thread count
    .excludeColumns("updated_at", "created_at")     // ignore columns
    .caseInsensitiveColumns(Set.of("email"))        // case-insensitive
    .numericTolerance(0.001)                        // float comparison
    .useChecksumCache(true)                         // enable caching
    .cacheMaxSize(50000)                            // cache size
    .build();
```

## Architecture

```
data-diff-java/
├── core/           # Core abstractions (models, SPI, strategies, exceptions)
├── engine/         # Comparison engines (HashDiff, JoinDiff, Bisector)
├── datasource/     # Database adapters (JDBC, dialects, connection pools)
├── hash/           # Hash functions (CRC32, MD5, Composite)
├── comparator/     # Row and column comparators
├── cache/          # Caching providers (Caffeine, LRU)
├── output/         # Result formatters (JSON, Table, CSV, HTML)
├── builder/        # Builder pattern implementations
├── function/       # Functional interfaces
└── util/           # Utilities (metrics, progress tracking)
```

## Supported Databases

- MySQL 8.0+
- PostgreSQL 12+
- Snowflake
- MariaDB (via MySQL dialect)

## Algorithms

### HashDiff
- Splits table into segments by primary key range
- Computes checksums for each segment
- Uses recursive bisection to locate differences
- Suitable for cross-database comparison

### JoinDiff
- Uses FULL OUTER JOIN for comparison
- Single SQL query completes the comparison
- Faster for same-database scenarios
- Requires both tables in same database

## Building

```bash
mvn clean install
```

## Testing

```bash
# Unit tests
mvn test

# Integration tests (requires Docker)
mvn verify -Pintegration
```

## License

MIT License

## Java Version

Requires Java 21+ for virtual threads and modern language features.

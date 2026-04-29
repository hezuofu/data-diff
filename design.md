# Data Diff Java - 架构设计文档

## 1. 项目概述

Data Diff Java是一个数据库表对比工具,实现了Python data-diff的核心算法,采用现代化Java架构设计。

### 1.1 设计目标

- **高性能**: 支持TB级表对比,并行处理,流式读取
- **可扩展**: SPI架构,支持自定义数据源、算法、输出格式
- **易用性**: 配置文件驱动,Builder API,清晰的错误信息
- **类型安全**: Java 17 Records,不可变对象,编译时检查

### 1.2 核心特性

| 特性 | 说明 |
|------|------|
| 双算法 | HashDiff(跨库) + JoinDiff(同库) |
| 配置管理 | Typesafe Config (HOCON格式) |
| 流式处理 | 避免OOM,支持大表 |
| 并行处理 | 3种策略适配不同场景 |
| 多数据库 | MySQL/PostgreSQL/Snowflake |
| SPI扩展 | 数据源/哈希/格式化器可插拔 |

## 2. 系统架构

### 2.1 分层架构

```
┌─────────────────────────────────────────────┐
│          Application Layer                  │
│  DataDiffApplication (CLI + Config)         │
└─────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────┐
│          Engine Layer                       │
│  DataDiffEngine → HashDiff/JoinDiff         │
└─────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────┐
│          Strategy Layer                     │
│  ComparisonStrategy / SplitStrategy         │
└─────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────┐
│          Datasource Layer                   │
│  JDBC + ConnectionPool + SQLDialect         │
└─────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────┐
│          Infrastructure Layer               │
│  Database (MySQL/PostgreSQL/Snowflake)      │
└─────────────────────────────────────────────┘
```

### 2.2 核心模块依赖

```
config → core.model
   ↓
engine → core.model, core.spi, core.strategy
   ↓         ↓              ↓              ↓
datasource  hash        comparator     output
   ↓         ↓              ↓              ↓
database   CRC32/MD5     Row/Column     JSON/CSV
```

## 3. 核心算法设计

### 3.1 HashDiff算法

**算法流程**:

```
1. 确定主键范围 [min_key, max_key]
2. 按segmentSize分段
   ↓
3. 对每个段:
   ├─ 计算左表checksum
   ├─ 计算右表checksum
   └─ 比较checksum
      ├─ 相同 → 跳过
      └─ 不同 → 递归二分
         ↓
4. 递归二分(RecursiveBisector):
   ├─ 将段分为两半
   ├─ 分别计算checksum
   └─ 对不匹配的半段继续二分
      ├─ 深度 < maxDepth → 继续递归
      └─ 深度 >= maxDepth → 提取差异行
         ↓
5. 提取差异行:
   ├─ 查询左表行
   ├─ 查询右表行
   └─ 逐行比较,生成DiffRecord
```

**关键设计**:

```java
// 分段模型
public record Segment(
    BigInteger rangeStart,      // 段起始主键
    BigInteger rangeEnd,        // 段结束主键
    long count,                 // 行数估计
    String checksum,            // 校验和
    int depth                   // 递归深度
) {
    // 二分算法
    public Segment[] bisect() {
        BigInteger mid = rangeStart.add(rangeEnd).divide(TWO);
        return new Segment[]{
            new Segment(rangeStart, mid, count/2, null, depth+1),
            new Segment(mid+1, rangeEnd, count-count/2, null, depth+1)
        };
    }
}

// 递归二分器
public class RecursiveBisector {
    private List<DiffRecord> bisect(Segment segment, int depth) {
        // 终止条件
        if (depth >= maxDepth || segment.count() <= 10) {
            return extractDiffRows(segment);
        }
        
        // 二分
        Segment[] halves = segment.bisect();
        
        // 递归处理
        for (Segment half : halves) {
            if (hasChecksumMismatch(half)) {
                diffs.addAll(bisect(half, depth + 1));
            }
        }
    }
}
```

**性能优化**:

1. **并行段处理**: 多个段同时计算checksum
2. **校验和缓存**: 使用Caffeine缓存已计算的段
3. **流式读取**: PartitionReader分批加载,避免OOM
4. **智能分段**: 根据表大小自动调整segmentSize

### 3.2 JoinDiff算法

**算法流程**:

```sql
-- 生成FULL OUTER JOIN查询
SELECT 
    COALESCE(l.id, r.id) as pk,
    CASE 
        WHEN l.id IS NULL THEN 'RIGHT_ONLY'
        WHEN r.id IS NULL THEN 'LEFT_ONLY'
        WHEN <比较列> THEN 'MODIFIED'
    END as diff_type,
    l.*, r.*
FROM left_table l
FULL OUTER JOIN right_table r ON l.id = r.id
WHERE l.id IS NULL 
   OR r.id IS NULL 
   OR <列值比较条件>
```

**Java实现**:

```java
public class JoinDiffEngine implements ComparisonStrategy {
    public DiffResult compare(...) {
        // 1. 生成JOIN SQL
        String sql = dialect.generateJoinDiffSql(leftTable, rightTable, options);
        
        // 2. 执行查询
        try (ResultSet rs = executor.executeQuery(conn, sql)) {
            // 3. 映射结果
            while (rs.next()) {
                DiffRecord record = mapRow(rs);
                diffs.add(record);
            }
        }
        
        return new DiffResult(diffs, statistics, duration);
    }
}
```

**适用场景对比**:

| 维度 | HashDiff | JoinDiff |
|------|----------|----------|
| 跨库支持 | ✅ | ❌ |
| 性能 | 中等(O(n log n)) | 快(O(n)) |
| 内存占用 | 低(流式) | 低(流式) |
| 网络开销 | 高(多次查询) | 低(单次查询) |
| 递归深度 | 需要控制 | 无递归 |

## 4. 核心模型设计

### 4.1 数据模型(Java Records)

```java
// 表信息
public record TableInfo(
    String name,                          // 表名
    List<ColumnDef> columns,              // 列定义
    List<String> primaryKey               // 主键列
) {}

// 列定义
public record ColumnDef(
    String name,                          // 列名
    String dataType,                      // 数据类型
    boolean nullable                      // 是否可空
) {
    public boolean isNumeric() {
        return dataType.matches("(?i)(BIGINT|INT|DECIMAL|FLOAT|DOUBLE|NUMERIC)");
    }
    
    public boolean isString() {
        return dataType.matches("(?i)(VARCHAR|CHAR|TEXT|STRING)");
    }
}

// 差异记录
public record DiffRecord(
    String primaryKeyString,              // 主键值
    DiffType diffType,                    // 差异类型
    Map<String, Object> leftData,         // 左表数据
    Map<String, Object> rightData,        // 右表数据
    List<String> differingColumns         // 差异列
) {}

// 差异类型
public enum DiffType {
    LEFT_ONLY,      // 仅左表存在
    RIGHT_ONLY,     // 仅右表存在
    MODIFIED        // 两侧都有但值不同
}

// 对比结果
public record DiffResult(
    List<DiffRecord> diffRecords,         // 差异记录
    Statistics statistics,                // 统计信息
    Duration duration                     // 耗时
) {
    public record Statistics(
        long leftRowCount,                // 左表行数
        long rightRowCount,               // 右表行数
        int segmentsCompared,             // 对比段数
        int bisectionIterations,          // 二分迭代次数
        int maxBisectionDepth,            // 最大二分深度
        int cacheHits                     // 缓存命中
    ) {}
    
    public int getDiffCount() {
        return diffRecords.size();
    }
    
    public boolean hasDifferences() {
        return !diffRecords.isEmpty();
    }
}
```

### 4.2 配置模型

```java
// 对比选项(Builder模式)
public final class CompareOptions {
    public enum Algorithm { HASHDIFF, JOINDIFF }
    
    private final Algorithm algorithm;
    private final int parallelism;
    private final long segmentSize;
    private final int maxBisectionDepth;
    private final List<String> excludeColumns;
    private final double numericTolerance;
    
    // Builder模式
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private Algorithm algorithm = Algorithm.HASHDIFF;
        private int parallelism = 4;
        private long segmentSize = 50000;
        
        public Builder algorithm(Algorithm algo) { ... }
        public Builder segmentSize(long size) { ... }
        public CompareOptions build() { ... }
    }
}
```

## 5. SPI扩展机制

### 5.1 SPI接口定义

```java
// 数据源提供者
public interface DataSourceProvider {
    boolean supports(String jdbcUrl);
    Connection getConnection(String url, Properties props);
    SqlDialect getDialect();
    String getProviderName();
}

// 校验和计算器
public interface ChecksumCalculator {
    String calculate(ResultSet rs, List<String> columns);
    String getAlgorithmName();
}

// 结果格式化器
public interface ResultFormatter {
    void format(DiffResult result, OutputStream outputStream);
    String getFormatName();
}
```

### 5.2 SPI注册

```
src/main/resources/META-INF/services/
├── io.sketch.datadiff.core.spi.ChecksumCalculator
│   # 内容:
│   io.sketch.datadiff.hash.Md5Checksum
│   io.sketch.datadiff.hash.Crc32Checksum
│
└── io.sketch.datadiff.core.spi.DataSourceProvider
    # 内容:
    io.sketch.datadiff.datasource.jdbc.JdbcDataSourceProvider
```

### 5.3 SPI加载

```java
public class HashFunctionRegistry {
    private static final Map<String, ChecksumCalculator> calculators;
    
    static {
        ServiceLoader<ChecksumCalculator> loader = 
            ServiceLoader.load(ChecksumCalculator.class);
        
        calculators = StreamSupport.stream(loader.spliterator(), false)
            .collect(Collectors.toMap(
                ChecksumCalculator::getAlgorithmName,
                c -> c
            ));
    }
    
    public static ChecksumCalculator get(String name) {
        return calculators.get(name);
    }
}
```

## 6. 数据库方言系统

### 6.1 方言接口

```java
public interface SqlDialect {
    // 生成分段查询SQL
    String generateSegmentQuerySql(
        TableInfo table,
        BigInteger startKey,
        BigInteger endKey,
        List<String> columns
    );
    
    // 生成校验和计算SQL
    String generateChecksumSql(
        TableInfo table,
        BigInteger startKey,
        BigInteger endKey,
        List<String> columns
    );
    
    // 生成JoinDiff SQL
    String generateJoinDiffSql(
        TableInfo leftTable,
        TableInfo rightTable,
        CompareOptions options
    );
    
    // 数据类型映射
    String mapDataType(String sqlType);
    
    // 引号处理
    String quoteIdentifier(String identifier);
}
```

### 6.2 MySQL方言实现

```java
public class MySqlDialect extends AbstractSqlDialect {
    
    @Override
    public String generateChecksumSql(...) {
        return """
            SELECT 
                BIT_XOR(CRC32(CONCAT_WS('|', %s))) as checksum,
                COUNT(*) as row_count
            FROM %s
            WHERE %s BETWEEN %d AND %d
            """.formatted(
            String.join(", ", columns),
            table.name(),
            primaryKey,
            startKey,
            endKey
        );
    }
    
    @Override
    public String quoteIdentifier(String id) {
        return "`" + id + "`";
    }
}
```

### 6.3 方言自动检测

```java
public class DialectResolver {
    public static SqlDialect resolve(String jdbcUrl) {
        if (jdbcUrl.contains("mysql")) {
            return new MySqlDialect();
        } else if (jdbcUrl.contains("postgresql")) {
            return new PostgreSqlDialect();
        } else if (jdbcUrl.contains("snowflake")) {
            return new SnowflakeDialect();
        }
        throw new UnsupportedOperationException("Unsupported database");
    }
}
```

## 7. 流式处理架构

### 7.1 分区读取器

```java
public class PartitionReader implements Iterator<Map<String, Object>> {
    private final int fetchSize = 10000;
    private List<Map<String, Object>> buffer;
    private int index = 0;
    
    @Override
    public boolean hasNext() {
        if (buffer == null || index >= buffer.size()) {
            return loadNextBatch();
        }
        return true;
    }
    
    private boolean loadNextBatch() {
        String sql = baseSql + " LIMIT " + fetchSize + " OFFSET " + (index);
        buffer = queryExecutor.executeQuery(conn, sql);
        index = 0;
        return !buffer.isEmpty();
    }
}
```

### 7.2 背压管理

```java
public class BackpressureManager {
    private final long maxMemoryBytes;
    private long currentMemoryBytes;
    private volatile boolean backpressureActive;
    
    public synchronized boolean recordMemoryIncrease(long bytes) {
        currentMemoryBytes += bytes;
        
        if (currentMemoryBytes >= maxMemoryBytes) {
            backpressureActive = true;
            log.warn("Backpressure activated: {} bytes", currentMemoryBytes);
        }
        
        return backpressureActive;
    }
    
    public void waitForBackpressureRelease() {
        while (backpressureActive) {
            Thread.sleep(100);
        }
    }
}
```

## 8. 并行处理策略

### 8.1 固定线程池

```java
public class ParallelSegmentProcessor {
    public List<DiffRecord> process(List<Segment> segments) {
        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        try {
            List<Future<List<DiffRecord>>> futures = segments.stream()
                .map(seg -> executor.submit(() -> processSegment(seg)))
                .toList();
            
            return futures.stream()
                .map(f -> f.get())
                .flatMap(List::stream)
                .toList();
        } finally {
            executor.shutdown();
        }
    }
}
```

### 8.2 ForkJoin工作窃取

```java
public class ForkJoinSegmentProcessor {
    private static class SegmentTask extends RecursiveTask<List<DiffRecord>> {
        protected List<DiffRecord> compute() {
            if (segments.size() <= 1) {
                return processSegment(segments.get(0));
            }
            
            int mid = segments.size() / 2;
            var leftTask = new SegmentTask(segments.subList(0, mid));
            var rightTask = new SegmentTask(segments.subList(mid, segments.size()));
            
            leftTask.fork();
            var rightResult = rightTask.compute();
            var leftResult = leftTask.join();
            
            return concat(leftResult, rightResult);
        }
    }
}
```

## 9. 配置系统设计

### 9.1 Typesafe Config架构

```
AppConfig (不可变对象)
  ├─ DatabaseConfig left
  ├─ DatabaseConfig right
  ├─ ComparisonConfig comparison
  └─ OutputConfig output

ConfigLoader (加载器)
  ├─ load(String path)  → 从文件加载
  ├─ loadDefault()      → 从classpath加载
  └─ validate()         → 配置验证
```

### 9.2 类型安全访问

```java
// 编译时类型检查
String url = config.left().url();           // 返回String
int parallelism = config.comparison().parallelism();  // 返回int
double tolerance = config.comparison().numericTolerance(); // 返回double

// 配置缺失时立即报错(而非运行时NPE)
// config.getString() throws ConfigException.Missing
```

### 9.3 HOCON高级特性

```hocon
# 1. 环境变量
password: ${DB_PASSWORD:"default"}

# 2. 路径引用
right.url: ${left.url}

# 3. 配置合并
include "base.conf"
include "production.conf"

# 4. 数组
excludeColumns: ["created_at", "updated_at"]

# 5. 对象
customComparators {
  status: "string"
  amount: "numeric"
}
```

## 10. 输出格式化

### 10.1 格式化器接口

```java
public interface ResultFormatter {
    void format(DiffResult result, OutputStream out);
    String getFormatName();
    
    default String formatToString(DiffResult result) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        format(result, baos);
        return baos.toString(UTF_8);
    }
}
```

### 10.2 JSON格式化器

```java
public class JsonFormatter implements ResultFormatter {
    private final ObjectMapper mapper;
    
    public JsonFormatter() {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    
    @Override
    public void format(DiffResult result, OutputStream out) {
        mapper.writeValue(out, result);
    }
}
```

## 11. 异常处理体系

```
DataDiffException (基础异常)
  ├─ ConnectionException           // 数据库连接失败
  ├─ ChecksumMismatchException     // 校验和计算异常
  └─ UnsupportedKeyTypeException   // 不支持的主键类型
```

所有异常都是RuntimeException,支持链式调用:

```java
try {
    // 对比逻辑
} catch (SQLException e) {
    throw new DataDiffException("对比失败", e);
}
```

## 12. 性能优化策略

### 12.1 校验和缓存

```java
public class CaffeineCacheProvider {
    private final Cache<String, String> cache;
    
    public CaffeineCacheProvider(int maxSize) {
        cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
    }
    
    public String getOrCompute(String key, Function<String, String> loader) {
        return cache.get(key, loader);
    }
}
```

### 12.2 分段大小调优

```
表大小 < 100万行     → segmentSize: 10000
表大小 100-1000万行  → segmentSize: 50000
表大小 > 1000万行    → segmentSize: 100000
```

### 12.3 并行度调优

```
CPU核心数 <= 4       → parallelism: 4
CPU核心数 4-8        → parallelism: 8
CPU核心数 > 8        → parallelism: CPU核心数
```

## 13. 测试策略

### 13.1 单元测试

- 哈希算法正确性测试
- 分段算法测试
- 比较器测试
- 配置加载测试

### 13.2 集成测试(TestContainers)

```java
@Testcontainers
class HashDiffEngineTest {
    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0");
    
    @Test
    void testCompareIdenticalTables() {
        // 创建相同表
        // 执行对比
        // 验证无差异
    }
}
```

### 13.3 性能基准测试

```java
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class HashDiffBenchmark {
    @Benchmark
    public DiffResult benchmarkLargeTable() {
        // 对比100万行表
        return engine.compare(leftTable, rightTable, options);
    }
}
```

## 14. 扩展指南

### 14.1 添加新的数据库方言

```java
// 1. 实现SqlDialect接口
public class OracleDialect implements SqlDialect {
    // 实现所有方法
}

// 2. 注册到DialectResolver
public class DialectResolver {
    public static SqlDialect resolve(String url) {
        if (url.contains("oracle")) {
            return new OracleDialect();
        }
        // ...
    }
}
```

### 14.2 添加新的哈希算法

```java
// 1. 实现ChecksumCalculator
public class Sha256Checksum implements ChecksumCalculator {
    @Override
    public String calculate(ResultSet rs, List<String> columns) {
        // SHA256实现
    }
    
    @Override
    public String getAlgorithmName() {
        return "sha256";
    }
}

// 2. 注册SPI
// META-INF/services/io.sketch.datadiff.core.spi.ChecksumCalculator
// 添加: io.sketch.datadiff.hash.Sha256Checksum
```

### 14.3 添加新的输出格式

```java
// 1. 实现ResultFormatter
public class XmlFormatter implements ResultFormatter {
    @Override
    public void format(DiffResult result, OutputStream out) {
        // XML输出逻辑
    }
    
    @Override
    public String getFormatName() {
        return "xml";
    }
}
```

## 15. 部署与运维

### 15.1 打包部署

```bash
# 构建可执行JAR
mvn clean package -DskipTests

# 运行
java -jar data-diff.jar --config production.conf
```

### 15.2 日志配置

```xml
<!-- log4j2.xml -->
<Configuration>
    <Appenders>
        <Console name="Console">
            <PatternLayout pattern="%d{HH:mm:ss} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
        <File name="File" fileName="logs/datadiff.log">
            <PatternLayout pattern="%d [%t] %-5level %logger{36} - %msg%n"/>
        </File>
    </Appenders>
    <Loggers>
        <Root level="info">
            <AppenderRef ref="Console"/>
            <AppenderRef ref="File"/>
        </Root>
    </Loggers>
</Configuration>
```

### 15.3 监控指标

```java
public class MetricsCollector {
    private AtomicLong segmentsProcessed = new AtomicLong();
    private AtomicLong cacheHits = new AtomicLong();
    private AtomicLong cacheMisses = new AtomicLong();
    
    public void reportMetrics() {
        log.info("Segments: {}, Cache Hit Rate: {}%",
            segmentsProcessed.get(),
            hitRate()
        );
    }
}
```

## 16. 总结

Data Diff Java通过以下设计实现高性能、可扩展的数据库表对比:

1. **双算法引擎**适配不同场景
2. **SPI架构**保证可扩展性
3. **流式处理**支持大表
4. **并行处理**提升性能
5. **类型安全配置**降低错误率
6. **函数式编程**提高代码质量

---

**作者**: lanxia39@163.com  
**版本**: 1.0.0  
**Java版本**: 17+

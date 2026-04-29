# Data Diff Java - 项目结构

## 目录结构

```
data-diff-java/
├── pom.xml                                    # Maven配置
├── application.conf                           # Typesafe Config配置文件
├── README.md                                  # 项目说明文档
│
├── src/main/java/io/sketch/datadiff/
│   ├── DataDiffApplication.java               # 主程序入口(支持配置文件启动)
│   │
│   ├── config/                                # 配置模块
│   │   ├── AppConfig.java                     # Typesafe Config包装器
│   │   └── ConfigLoader.java                  # 配置加载与验证
│   │
│   ├── core/                                  # 核心抽象模块
│   │   ├── model/                             # 数据模型
│   │   │   ├── TableInfo.java                 # 表信息
│   │   │   ├── Segment.java                   # 分段(含二分算法)
│   │   │   ├── DiffRecord.java                # 差异记录
│   │   │   ├── DiffResult.java                # 对比结果(含Statistics)
│   │   │   ├── DiffType.java                  # 差异类型枚举
│   │   │   ├── CompareOptions.java            # 对比选项(Builder模式)
│   │   │   └── ColumnDef.java                 # 列定义
│   │   │
│   │   ├── spi/                               # 服务提供者接口
│   │   │   ├── DataSourceProvider.java        # 数据源提供者
│   │   │   ├── RowMapper.java                 # 行映射器
│   │   │   ├── ChecksumCalculator.java        # 校验和计算器
│   │   │   └── ResultFormatter.java           # 结果格式化器
│   │   │
│   │   ├── strategy/                          # 策略接口
│   │   │   ├── ComparisonStrategy.java        # 对比策略
│   │   │   ├── SplitStrategy.java             # 分段策略
│   │   │   ├── KeyExtractor.java              # 键提取器
│   │   │   └── OrderingStrategy.java          # 排序策略
│   │
│   ├── engine/                                # 核心引擎模块
│   │   ├── DataDiffEngine.java                # 对比引擎入口
│   │   ├── HashDiffEngine.java                # HashDiff算法(跨库)
│   │   ├── JoinDiffEngine.java                # JoinDiff算法(同库)
│   │   ├── RecursiveBisector.java             # 递归二分定位器
│   │   ├── SegmentSplitter.java               # 分段器
│   │   ├── ChunkProcessor.java                # 块处理器
│   │   └── StreamingComparator.java           # 流式对比器
│   │
│   ├── datasource/                            # 数据源适配模块
│   │   ├── jdbc/                              # JDBC实现
│   │   │   ├── JdbcDataSourceProvider.java    # JDBC数据源提供者
│   │   │   └── JdbcQueryExecutor.java         # JDBC查询执行器
│   │   ├── pool/                              # 连接池
│   │   │   ├── HikariCPProvider.java          # HikariCP提供者
│   │   │   └── ConnectionPoolManager.java     # 连接池管理器
│   │   └── dialect/                           # 数据库方言
│   │       ├── SqlDialect.java                # 方言接口
│   │       ├── AbstractSqlDialect.java        # 抽象实现
│   │       ├── MySqlDialect.java              # MySQL方言
│   │       ├── PostgreSqlDialect.java         # PostgreSQL方言
│   │       ├── SnowflakeDialect.java          # Snowflake方言
│   │       └── DialectResolver.java           # 方言解析器
│   │
│   ├── hash/                                  # 哈希计算模块
│   │   ├── Md5Checksum.java                   # MD5实现
│   │   ├── Crc32Checksum.java                 # CRC32实现
│   │   ├── CompositeChecksum.java             # 复合列哈希
│   │   └── HashFunctionRegistry.java          # 哈希函数注册表
│   │
│   ├── comparator/                            # 比较器模块
│   │   ├── ColumnComparator.java              # 列比较器
│   │   ├── RowComparator.java                 # 行比较器
│   │   ├── NumericToleranceComparator.java    # 数值容差比较
│   │   ├── StringNormalizingComparator.java   # 字符串归一化比较
│   │   └── CompositeComparator.java           # 组合比较器
│   │
│   ├── stream/                                # 流式处理模块
│   │   ├── PartitionReader.java               # 分区读取器
│   │   ├── RowPublisher.java                  # 行发布器(Flow.Publisher)
│   │   ├── BackpressureManager.java           # 背压管理器
│   │   └── StreamMerger.java                  # 流合并器
│   │
│   ├── parallel/                              # 并行处理模块
│   │   ├── ParallelSegmentProcessor.java      # 固定线程池处理器
│   │   ├── ForkJoinSegmentProcessor.java      # ForkJoin处理器
│   │   ├── ThreadPoolConfig.java              # 线程池配置
│   │   └── WorkStealingStrategy.java          # 工作窃取策略
│   │
│   ├── cache/                                 # 缓存模块
│   │   ├── CaffeineCacheProvider.java         # Caffeine缓存提供者
│   │   └── LRUCache.java                      # LRU缓存实现
│   │
│   ├── output/                                # 输出格式化模块
│   │   ├── JsonFormatter.java                 # JSON格式化
│   │   ├── TableFormatter.java                # 表格格式化
│   │   ├── CsvFormatter.java                  # CSV格式化
│   │   └── StatsFormatter.java                # 统计信息格式化
│   │
│   ├── function/                              # 函数式接口
│   │   └── ThrowingFunction.java              # 异常处理函数
│   │
│   ├── builder/                               # 构建器
│   │   └── DataDiffBuilder.java               # DataDiff构建器
│   │
│   ├── exception/                             # 异常体系
│   │   ├── DataDiffException.java             # 基础异常
│   │   ├── ConnectionException.java           # 连接异常
│   │   ├── ChecksumMismatchException.java     # 校验和不匹配异常
│   │   └── UnsupportedKeyTypeException.java   # 不支持的键类型异常
│   │
│   └── util/                                  # 工具类
│       ├── IdRangeCalculator.java             # ID范围计算器
│       └── MetricsCollector.java              # 指标收集器
│
├── src/main/resources/
│   ├── META-INF/services/                     # SPI服务配置
│   │   ├── io.sketch.datadiff.core.spi.ChecksumCalculator
│   │   └── io.sketch.datadiff.core.spi.DataSourceProvider
│   └── log4j2.xml                             # 日志配置
│
└── src/test/java/io/sketch/datadiff/          # 测试代码(待实现)
    ├── engine/
    ├── datasource/
    └── integration/
```

## 核心架构

### 双算法引擎
- **HashDiff**: 基于校验和+递归二分,支持跨库对比
- **JoinDiff**: 基于FULL OUTER JOIN,同库优化

### SPI扩展点
- DataSourceProvider: 数据源扩展
- ChecksumCalculator: 哈希算法扩展
- ResultFormatter: 输出格式扩展

### 技术栈
- Java 17 (Records, Stream API, Switch表达式)
- Typesafe Config (HOCON配置格式)
- HikariCP (连接池)
- Caffeine (缓存)
- Jackson (JSON序列化)
- Maven (构建工具)

## 使用方式

```bash
# 使用默认配置
java -jar data-diff.jar

# 使用自定义配置
java -jar data-diff.jar --config my-config.conf

# 查看帮助
java -jar data-diff.jar --help
```

## 作者

lanxia39@163.com

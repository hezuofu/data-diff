# Data Diff Java

基于Java 17的数据库表对比工具,支持同库和跨库数据差异检测。

## 特性

- **双算法引擎**: HashDiff(跨库对比)和JoinDiff(同库优化)
- **类型安全配置**: Typesafe Config (HOCON格式),支持环境变量和配置合并
- **SPI扩展架构**: 数据源、哈希算法、输出格式可插拔
- **流式处理**: 支持TB级大表,避免OOM
- **并行处理**: 固定线程池、ForkJoinPool、WorkStealing三种策略
- **多数据库支持**: MySQL、PostgreSQL、Snowflake自动方言检测
- **高级比较**: 数值容差、字符串归一化、列排除
- **多种输出**: JSON、CSV、表格、统计信息
- **函数式友好**: Records、Stream API、Lambda表达式

## 快速开始

### 方式一: 使用配置文件(推荐)

1. 创建配置文件 `my-config.conf`:

```hocon
left {
  url: "jdbc:mysql://localhost:3306/database1"
  username: "root"
  password: "your_password"
  table: "users"
  primaryKey: ["id"]
}

right {
  url: "jdbc:mysql://localhost:3306/database2"
  username: "root"
  password: "your_password"
  table: "users"
  primaryKey: ["id"]
}

comparison {
  algorithm: "hashdiff"
  segmentSize: 50000
  parallelism: 4
  numericTolerance: 0.001
}

output {
  format: "json"
  outputFile: "diff-result.json"
  showStats: true
}
```

2. 运行对比:

```bash
java -jar data-diff.jar --config my-config.conf
```

### 方式二: 编程式API

```java
import io.sketch.datadiff.config.AppConfig;
import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.engine.DataDiffEngine;
import io.sketch.datadiff.engine.HashDiffEngine;
import io.sketch.datadiff.datasource.pool.HikariCPProvider;
import io.sketch.datadiff.output.JsonFormatter;

import javax.sql.DataSource;
import java.util.List;
import java.util.Properties;

public class DataDiffExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建数据源
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "password");
        
        DataSource leftDs = HikariCPProvider.createDataSource(
            "jdbc:mysql://localhost:3306/db1", props
        );
        
        DataSource rightDs = HikariCPProvider.createDataSource(
            "jdbc:mysql://localhost:3306/db2", props
        );
        
        // 2. 定义表结构
        TableInfo leftTable = new TableInfo(
            "users",
            List.of(), // 列自动检测
            List.of("id")
        );
        
        TableInfo rightTable = new TableInfo(
            "users",
            List.of(),
            List.of("id")
        );
        
        // 3. 配置对比选项
        CompareOptions options = CompareOptions.builder()
            .algorithm(CompareOptions.Algorithm.HASHDIFF)
            .segmentSize(50000)
            .parallelism(8)
            .numericTolerance(0.001)
            .excludeColumns("updated_at", "created_at")
            .build();
        
        // 4. 创建引擎并执行对比
        DataDiffEngine engine = DataDiffEngine.create(
            leftDs, rightDs, new HashDiffEngine()
        );
        
        DiffResult result = engine.compare(leftTable, rightTable, options);
        
        // 5. 输出结果
        if (result.hasDifferences()) {
            System.out.println("发现 " + result.getDiffCount() + " 处差异");
            new JsonFormatter().format(result, System.out);
        } else {
            System.out.println("表数据一致");
        }
    }
}
```

## 命令行使用

```bash
# 使用默认配置 (application.conf)
java -jar data-diff.jar

# 使用自定义配置
java -jar data-diff.jar --config my-config.conf

# 查看帮助
java -jar data-diff.jar --help
```

## 配置说明

### HOCON配置格式

Typesafe Config支持强大的配置语法:

```hocon
# 环境变量引用
left {
  password: ${DB_PASSWORD:"default_password"}
}

# 配置复用
right {
  url: ${left.url}
  table: ${left.table}
}

# 包含其他配置文件
include "base.conf"

# 注释支持
comparison {
  algorithm: "hashdiff"  # hashdiff 或 joindiff
}
```

### 完整配置示例

参考 [application.conf](application.conf) 文件。

## 架构设计

### 核心模块

```
io.sketch.datadiff/
├── config/              # 配置模块 (Typesafe Config)
│   ├── AppConfig        # 类型安全配置对象
│   └── ConfigLoader     # 配置加载与验证
│
├── core/                # 核心抽象
│   ├── model/           # 数据模型 (Records)
│   │   ├── TableInfo        # 表信息
│   │   ├── Segment          # 分段(含二分算法)
│   │   ├── DiffRecord       # 差异记录
│   │   ├── DiffResult       # 对比结果
│   │   ├── DiffType         # 差异类型
│   │   ├── CompareOptions   # 对比选项
│   │   └── ColumnDef        # 列定义
│   │
│   ├── spi/             # SPI接口
│   │   ├── DataSourceProvider    # 数据源提供者
│   │   ├── ChecksumCalculator    # 校验和计算器
│   │   ├── ResultFormatter       # 结果格式化器
│   │   └── RowMapper             # 行映射器
│   │
│   └── strategy/        # 策略接口
│       ├── ComparisonStrategy    # 对比策略
│       ├── SplitStrategy         # 分段策略
│       ├── KeyExtractor          # 键提取器
│       └── OrderingStrategy      # 排序策略
│
├── engine/              # 核心引擎
│   ├── DataDiffEngine         # 引擎入口
│   ├── HashDiffEngine         # HashDiff算法
│   ├── JoinDiffEngine         # JoinDiff算法
│   ├── RecursiveBisector      # 递归二分定位
│   ├── SegmentSplitter        # 分段器
│   ├── ChunkProcessor         # 块处理器
│   └── StreamingComparator    # 流式对比器
│
├── datasource/          # 数据源适配
│   ├── jdbc/            # JDBC实现
│   ├── pool/            # 连接池(HikariCP)
│   └── dialect/         # 数据库方言
│       ├── SqlDialect           # 方言接口
│       ├── MySqlDialect         # MySQL
│       ├── PostgreSqlDialect    # PostgreSQL
│       └── SnowflakeDialect     # Snowflake
│
├── hash/                # 哈希计算
│   ├── Md5Checksum            # MD5
│   ├── Crc32Checksum          # CRC32
│   ├── CompositeChecksum      # 复合哈希
│   └── HashFunctionRegistry   # 哈希注册表
│
├── comparator/          # 比较器
│   ├── ColumnComparator       # 列比较
│   ├── RowComparator          # 行比较
│   ├── NumericToleranceComparator  # 数值容差
│   ├── StringNormalizingComparator # 字符串归一化
│   └── CompositeComparator    # 组合比较
│
├── stream/              # 流式处理
│   ├── PartitionReader        # 分区读取
│   ├── RowPublisher           # 行发布(Flow.Publisher)
│   ├── BackpressureManager    # 背压管理
│   └── StreamMerger           # 流合并
│
├── parallel/            # 并行处理
│   ├── ParallelSegmentProcessor   # 固定线程池
│   ├── ForkJoinSegmentProcessor   # ForkJoin
│   ├── ThreadPoolConfig           # 线程池配置
│   └── WorkStealingStrategy       # 工作窃取
│
├── cache/               # 缓存
│   ├── CaffeineCacheProvider  # Caffeine缓存
│   └── LRUCache               # LRU缓存
│
├── output/              # 输出格式化
│   ├── JsonFormatter          # JSON
│   ├── CsvFormatter           # CSV
│   ├── TableFormatter         # 表格
│   └── StatsFormatter         # 统计信息
│
├── function/            # 函数式接口
│   └── ThrowingFunction       # 异常处理函数
│
├── builder/             # 构建器
│   └── DataDiffBuilder        # 引擎构建器
│
├── exception/           # 异常体系
│   ├── DataDiffException          # 基础异常
│   ├── ConnectionException        # 连接异常
│   ├── ChecksumMismatchException  # 校验和异常
│   └── UnsupportedKeyTypeException # 键类型异常
│
└── util/                # 工具类
    ├── IdRangeCalculator      # ID范围计算
    └── MetricsCollector       # 指标收集
```

### 双算法引擎

#### HashDiff (跨库对比)

**工作流程**:
1. 按主键范围将表分段
2. 计算每个段的校验和
3. 比较校验和,定位不匹配的段
4. 递归二分到单行级别
5. 提取具体差异行

**适用场景**:
- 跨数据库对比
- 跨服务器对比
- 大数据量表

**性能优化**:
- 并行段处理
- 校验和缓存
- 流式读取

#### JoinDiff (同库优化)

**工作流程**:
1. 生成FULL OUTER JOIN SQL
2. 单次查询完成对比
3. 直接输出差异行

**适用场景**:
- 同一数据库内的表对比
- 小数据量表
- 需要详细差异信息

**优势**:
- 单次查询,速度快
- 无需递归
- SQL层面优化

### SPI扩展点

项目通过SPI机制支持扩展:

1. **DataSourceProvider**: 自定义数据源
2. **ChecksumCalculator**: 自定义哈希算法
3. **ResultFormatter**: 自定义输出格式

SPI配置文件位于 `src/main/resources/META-INF/services/`

## 支持的数据库

- ✅ MySQL 8.0+
- ✅ PostgreSQL 12+
- ✅ Snowflake
- ✅ MariaDB (通过MySQL方言)

## 构建与开发

### 环境要求

- Java 17+
- Maven 3.6+

### 构建项目

```bash
# 编译
mvn clean compile

# 打包(生成可执行JAR)
mvn clean package -DskipTests

# 运行测试
mvn test

# 安装到本地仓库
mvn clean install
```

### Maven依赖

```xml
<dependency>
    <groupId>io.sketch.datadiff</groupId>
    <artifactId>data-diff-java</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 核心依赖

- **Typesafe Config 1.4.3**: HOCON配置
- **HikariCP 5.1.0**: 连接池
- **Caffeine 3.1.8**: 缓存
- **Jackson 2.16.1**: JSON序列化
- **SLF4J 2.0.11**: 日志门面
- **Log4j2 2.22.1**: 日志实现

## 项目结构

```
data-diff-java/
├── src/main/java/io/sketch/datadiff/   # 源代码 (67个Java文件)
├── src/main/resources/                  # 资源文件
│   ├── META-INF/services/               # SPI配置
│   └── log4j2.xml                       # 日志配置
├── src/test/java/io/sketch/datadiff/    # 测试代码
├── application.conf                     # 配置示例
├── pom.xml                              # Maven配置
└── README.md                            # 项目文档
```

## 最佳实践

### 1. 大表对比

```hocon
comparison {
  segmentSize: 100000      # 增大分段大小
  parallelism: 8           # 增加并行度
  maxBisectionDepth: 15    # 限制递归深度
}
```

### 2. 忽略时间戳列

```hocon
left {
  excludeColumns: ["created_at", "updated_at", "modified_time"]
}
```

### 3. 浮点数容差

```hocon
comparison {
  numericTolerance: 0.001  # 允许0.001的误差
}
```

### 4. 环境变量配置

```hocon
left {
  url: ${DATABASE_URL}
  username: ${DB_USER:"root"}
  password: ${DB_PASSWORD}
}
```

## 常见问题

**Q: HashDiff和JoinDiff如何选择?**

A: 同库使用JoinDiff(更快),跨库必须用HashDiff。

**Q: 如何调优性能?**

A: 增加`segmentSize`和`parallelism`,启用校验和缓存。

**Q: 支持哪些主键类型?**

A: BIGINT, INT, UUID, VARCHAR等可排序类型。

## 作者

lanxia39@163.com

## 许可证

MIT License

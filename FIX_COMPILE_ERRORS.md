# 编译错误修复脚本

本文档提供逐步修复编译错误的命令。

## 错误分类

### 1. DiffType枚举分离 (1个错误)
### 2. Jackson依赖缺失 (2个错误)
### 3. BigInteger导入缺失 (17个错误)
### 4. Statistics类引用 (4个错误)
### 5. 虚拟线程API (2个错误)
### 6. 其他问题 (14个错误)

## 修复方案

由于错误较多,建议手动修复以下关键文件:

### 文件1: 提取DiffType枚举
```bash
# 创建新文件
```

创建 `src/main/java/com/datadiff/core/model/DiffType.java`:
```java
package com.datadiff.core.model;

public enum DiffType {
    LEFT_ONLY,
    RIGHT_ONLY,
    MODIFIED
}
```

然后从DiffRecord.java中删除DiffType枚举定义(第8-15行)。

### 文件2: 添加Jackson依赖
在pom.xml的dependencies中添加:
```xml
<dependency>
    <groupId>com.fasterxml.jackson.datatype</groupId>
    <artifactId>jackson-datatype-jsr310</artifactId>
    <version>${jackson.version}</version>
</dependency>
```

### 文件3: StreamingComparator.java添加BigInteger导入
在文件顶部添加:
```java
import java.math.BigInteger;
```

### 文件4: HashDiffEngine.java修复
1. 添加导入: `import java.math.BigInteger;`
2. 替换虚拟线程为固定线程池(第100行):
```java
// 替换前:
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

// 替换后:
ExecutorService executor = Executors.newFixedThreadPool(options.getParallelism());
try {
```

并在finally中关闭:
```java
} finally {
    executor.shutdown();
}
```

3. 修正Statistics引用(第75行):
```java
DiffResult.Statistics stats = new DiffResult.Statistics(...);
```

### 文件5: JoinDiffEngine.java修复
修正Statistics引用(第53行):
```java
DiffResult.Statistics stats = new DiffResult.Statistics(...);
```

### 文件6: DataDiffEngine.java修改可见性
将create方法改为public(第76行):
```java
public static DataDiffEngine create(...) {
```

### 文件7: RecursiveBisector.java修复变量引用
第143-144行,将`table`改为`leftTable`:
```java
String pkColumn = leftTable.primaryKey().get(0);
List<String> compareColumns = leftTable.columns()...
```

### 文件8: ResultFormatter.java移除注解
删除第9行的`@FunctionalInterface`注解。

### 文件9: RowPublisher.java修复泛型
第23行,添加泛型类型:
```java
this.publisher = new SubmissionPublisher<DiffRecord>(maxBufferCapacity);
```

### 文件10: StreamMerger.java修复类型转换
第106行,添加类型转换:
```java
return (Comparable<?>) pk;
```

## 快速修复优先级

建议按以下顺序修复:

1. **高优先级** (阻塞编译):
   - DiffType枚举分离
   - BigInteger导入
   - Statistics引用修正
   - 虚拟线程替换

2. **中优先级** (功能完善):
   - Jackson依赖
   - 方法可见性
   - 变量引用修正

3. **低优先级** (代码质量):
   - 移除多余注解
   - 泛型类型完善

## 预期修复时间

熟练开发者: 20-30分钟
普通开发者: 40-60分钟

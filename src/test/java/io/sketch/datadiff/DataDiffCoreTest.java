package io.sketch.datadiff;

import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.core.spi.ChecksumCalculator;
import io.sketch.datadiff.hash.*;
import io.sketch.datadiff.util.MetricsCollector;
import io.sketch.datadiff.config.AppConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for data-diff core functionality.
 *
 * @author lanxia39@163.com
 */
@DisplayName("Data Diff 核心功能测试")
class DataDiffCoreTest {

    @Test
    @DisplayName("模型测试 - TableInfo")
    void testTableInfo() {
        List<ColumnDef> columns = List.of(
            new ColumnDef("id", "BIGINT", false),
            new ColumnDef("name", "VARCHAR", true)
        );
        
        TableInfo table = new TableInfo("users", null, columns, List.of("id"), List.of());
        
        assertEquals("users", table.tableName());
        assertEquals(2, table.columns().size());
        assertEquals(1, table.primaryKey().size());
    }

    @Test
    @DisplayName("模型测试 - ColumnDef类型判断")
    void testColumnDefTypes() {
        ColumnDef numeric = new ColumnDef("amount", "BIGINT", false);
        ColumnDef string = new ColumnDef("name", "VARCHAR", false);
        
        assertTrue(numeric.isNumeric());
        assertFalse(numeric.isString());
        
        assertFalse(string.isNumeric());
        assertTrue(string.isString());
    }

    @Test
    @DisplayName("模型测试 - DiffRecord")
    void testDiffRecord() {
        DiffRecord record = new DiffRecord(
            Map.of("id", 1L),
            Map.of("id", 1L, "name", "Alice"),
            Map.of("id", 1L, "name", "Bob"),
            DiffType.MODIFIED,
            List.of("name")
        );
        
        assertEquals(DiffType.MODIFIED, record.diffType());
        assertEquals(1, record.differingColumns().size());
        assertEquals("name", record.differingColumns().get(0));
    }

    @Test
    @DisplayName("模型测试 - DiffResult统计")
    void testDiffResult() {
        List<DiffRecord> diffs = List.of(
            new DiffRecord(Map.of("id", 1L), Map.of(), Map.of(), DiffType.MODIFIED, List.of())
        );
        
        DiffResult.Statistics stats = new DiffResult.Statistics(
            1000, 1050, 10, 25, 5, 3
        );
        
        DiffResult result = new DiffResult(diffs, stats, Duration.ofSeconds(5));
        
        assertTrue(result.hasDifferences());
        assertEquals(1, result.getDiffCount());
        assertEquals(1000, result.getStatistics().leftRowCount());
    }

    @Test
    @DisplayName("模型测试 - CompareOptions Builder")
    void testCompareOptions() {
        CompareOptions options = CompareOptions.builder()
            .algorithm(CompareOptions.Algorithm.HASHDIFF)
            .segmentSize(50000)
            .parallelism(8)
            .maxBisectionDepth(20)
            .numericTolerance(0.001)
            .excludeColumns("created_at", "updated_at")
            .build();
        
        assertEquals(CompareOptions.Algorithm.HASHDIFF, options.getAlgorithm());
        assertEquals(50000, options.getSegmentSize());
        assertEquals(8, options.getParallelism());
        assertEquals(2, options.getExcludeColumns().size());
    }

    @Test
    @DisplayName("模型测试 - Segment二分算法")
    void testSegmentBisect() {
        Segment segment = new Segment(
            BigInteger.valueOf(1),
            BigInteger.valueOf(100),
            100,
            null,
            0
        );
        
        Segment[] halves = segment.bisect();
        
        assertEquals(2, halves.length);
        assertEquals(BigInteger.valueOf(1), halves[0].rangeStart());
        assertEquals(BigInteger.valueOf(50), halves[0].rangeEnd());
        assertEquals(BigInteger.valueOf(51), halves[1].rangeStart());
        assertEquals(BigInteger.valueOf(100), halves[1].rangeEnd());
        assertEquals(1, halves[0].depth());
    }

    @Test
    @DisplayName("哈希测试 - MD5")
    void testMd5Checksum() {
        Md5Checksum md5 = new Md5Checksum();
        
        BigInteger hash1 = md5.checksum("test");
        BigInteger hash2 = md5.checksum("test");
        BigInteger hash3 = md5.checksum("different");
        
        assertEquals(hash1, hash2);
        assertNotEquals(hash1, hash3);
        assertNotNull(hash1);
    }

    @Test
    @DisplayName("哈希测试 - CRC32")
    void testCrc32Checksum() {
        Crc32Checksum crc32 = new Crc32Checksum();
        
        BigInteger hash1 = crc32.checksum("test");
        BigInteger hash2 = crc32.checksum("test");
        BigInteger hash3 = crc32.checksum("different");
        
        assertEquals(hash1, hash2);
        assertNotEquals(hash1, hash3);
    }

    @Test
    @DisplayName("哈希测试 - 注册表")
    void testHashFunctionRegistry() {
        ChecksumCalculator md5 = HashFunctionRegistry.get("MD5");
        ChecksumCalculator crc32 = HashFunctionRegistry.get("CRC32");
        
        assertNotNull(md5);
        assertNotNull(crc32);
        assertEquals("MD5", md5.getAlgorithmName());
        assertEquals("CRC32", crc32.getAlgorithmName());
    }

    @Test
    @DisplayName("工具测试 - MetricsCollector")
    void testMetricsCollector() {
        MetricsCollector metrics = new MetricsCollector();
        metrics.start();
        
        metrics.incrementRowsProcessed(100);
        metrics.incrementRowsProcessed(200);
        metrics.incrementSegmentsCompared();
        metrics.incrementSegmentsCompared();
        metrics.incrementChecksumsComputed();
        
        metrics.stop();
        
        assertEquals(300, metrics.getRowsProcessed());
        assertEquals(2, metrics.getSegmentsCompared());
        assertEquals(1, metrics.getChecksumsComputed());
        assertTrue(metrics.getDuration().toMillis() >= 0);
    }

    @Test
    @DisplayName("工具测试 - MetricsCollector性能计算")
    void testMetricsPerformance() throws InterruptedException {
        MetricsCollector metrics = new MetricsCollector();
        metrics.start();
        
        metrics.incrementRowsProcessed(1000);
        
        Thread.sleep(100);
        metrics.stop();
        
        double rate = metrics.getRowsPerSecond();
        assertTrue(rate > 0, "Should calculate positive rate");
    }

    @Test
    @DisplayName("配置测试 - 加载默认配置")
    void testLoadDefaultConfig() {
        assertDoesNotThrow(() -> {
            AppConfig config = AppConfig.load();
            assertNotNull(config);
        });
    }

    @Test
    @DisplayName("配置测试 - CompareOptions默认值")
    void testCompareOptionsDefaults() {
        CompareOptions options = CompareOptions.builder().build();
        
        assertEquals(CompareOptions.Algorithm.HASHDIFF, options.getAlgorithm());
        assertEquals(50000, options.getSegmentSize());
        assertEquals(20, options.getParallelism()); // 默认是20
        assertEquals(0.0, options.getNumericTolerance());
        assertTrue(options.getExcludeColumns().isEmpty());
    }

    @Test
    @DisplayName("集成测试 - 完整工作流")
    void testCompleteWorkflow() {
        // 1. 创建表信息
        TableInfo table = new TableInfo(
            "users",
            null,
            List.of(new ColumnDef("id", "BIGINT", false)),
            List.of("id"),
            List.of()
        );
        
        // 2. 配置对比选项
        CompareOptions options = CompareOptions.builder()
            .algorithm(CompareOptions.Algorithm.HASHDIFF)
            .segmentSize(1000)
            .parallelism(2)
            .build();
        
        // 3. 创建指标收集器
        MetricsCollector metrics = new MetricsCollector();
        metrics.start();
        
        // 4. 计算校验和
        Md5Checksum md5 = new Md5Checksum();
        BigInteger checksum = md5.checksum("test_data");
        metrics.incrementChecksumsComputed();
        
        metrics.stop();
        
        // 验证
        assertNotNull(checksum);
        assertEquals(1, metrics.getChecksumsComputed());
        assertEquals("users", table.tableName());
        assertEquals(CompareOptions.Algorithm.HASHDIFF, options.getAlgorithm());
    }
}

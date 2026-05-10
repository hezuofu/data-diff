package io.sketch.datadiff.hash;

import io.sketch.datadiff.core.spi.ChecksumCalculator;
import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class CompositeChecksumTest {

    @Test
    void testChecksumConsistency() {
        ChecksumCalculator calc1 = new Crc32Checksum();
        ChecksumCalculator calc2 = new Md5Checksum();
        CompositeChecksum composite = new CompositeChecksum(calc1, calc2);
        
        BigInteger hash1 = composite.checksum("test");
        BigInteger hash2 = composite.checksum("test");
        assertEquals(hash1, hash2);
    }

    @Test
    void testAlgorithmName() {
        ChecksumCalculator calc1 = new Crc32Checksum();
        ChecksumCalculator calc2 = new Md5Checksum();
        CompositeChecksum composite = new CompositeChecksum(calc1, calc2);
        
        assertEquals("Composite[CRC32+MD5]", composite.getAlgorithmName());
    }

    @Test
    void testValidation() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeChecksum(List.of()));
    }
}

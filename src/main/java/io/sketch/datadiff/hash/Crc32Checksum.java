package io.sketch.datadiff.hash;

import io.sketch.datadiff.core.spi.ChecksumCalculator;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * CRC32-based checksum calculator (simple and fast).
 */
public class Crc32Checksum implements ChecksumCalculator {
    
    @Override
    public BigInteger checksum(Object value) {
        if (value == null) {
            return BigInteger.ZERO;
        }
        
        CRC32 crc = new CRC32();
        byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
        crc.update(bytes);
        return BigInteger.valueOf(crc.getValue());
    }
    
    @Override
    public String getAlgorithmName() {
        return "CRC32";
    }
}

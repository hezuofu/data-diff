package io.sketch.datadiff.core.spi;

import java.math.BigInteger;
import java.util.List;

/**
 * Service Provider Interface for checksum calculation.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public interface ChecksumCalculator {
    
    /**
     * Calculate checksum for a single value.
     * 
     * @param value value to checksum
     * @return checksum as BigInteger
     */
    BigInteger checksum(Object value);
    
    /**
     * Calculate composite checksum for multiple values.
     * 
     * @param values list of values to checksum
     * @return combined checksum as BigInteger
     */
    default BigInteger checksum(List<Object> values) {
        BigInteger result = BigInteger.ZERO;
        for (Object value : values) {
            BigInteger hash = checksum(value);
            result = result.multiply(BigInteger.valueOf(31)).add(hash);
        }
        return result;
    }
    
    /**
     * Get the name of this checksum algorithm.
     * 
     * @return algorithm name
     */
    String getAlgorithmName();
}

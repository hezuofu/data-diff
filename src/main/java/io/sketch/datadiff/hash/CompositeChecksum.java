package io.sketch.datadiff.hash;

import io.sketch.datadiff.core.spi.ChecksumCalculator;

import java.math.BigInteger;
import java.util.List;

/**
 * Composite checksum that combines multiple calculators for better distribution.
 */
public class CompositeChecksum implements ChecksumCalculator {
    
    private final List<ChecksumCalculator> calculators;
    
    public CompositeChecksum(List<ChecksumCalculator> calculators) {
        if (calculators == null || calculators.isEmpty()) {
            throw new IllegalArgumentException("At least one calculator is required");
        }
        this.calculators = List.copyOf(calculators);
    }
    
    public CompositeChecksum(ChecksumCalculator... calculators) {
        this(List.of(calculators));
    }
    
    @Override
    public BigInteger checksum(Object value) {
        BigInteger result = BigInteger.ZERO;
        for (ChecksumCalculator calc : calculators) {
            BigInteger hash = calc.checksum(value);
            result = result.multiply(BigInteger.valueOf(31)).add(hash);
        }
        return result.abs();
    }
    
    @Override
    public BigInteger checksum(List<Object> values) {
        BigInteger result = BigInteger.ZERO;
        for (ChecksumCalculator calc : calculators) {
            BigInteger hash = calc.checksum(values);
            result = result.multiply(BigInteger.valueOf(31)).add(hash);
        }
        return result.abs();
    }
    
    @Override
    public String getAlgorithmName() {
        return "Composite[" + 
            calculators.stream()
                .map(ChecksumCalculator::getAlgorithmName)
                .reduce((a, b) -> a + "+" + b)
                .orElse("None") + 
            "]";
    }
}

package io.sketch.datadiff.hash;

import io.sketch.datadiff.core.spi.ChecksumCalculator;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5-based checksum calculator.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class Md5Checksum implements ChecksumCalculator {
    
    @Override
    public BigInteger checksum(Object value) {
        if (value == null) {
            return BigInteger.ZERO;
        }
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            // Add type information to prevent collisions between different types with same string representation
            String typePrefix = value.getClass().getName() + ":";
            md.update(typePrefix.getBytes(StandardCharsets.UTF_8));
            
            byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
            byte[] hash = md.digest(bytes);
            return new BigInteger(1, hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }
    
    @Override
    public String getAlgorithmName() {
        return "MD5";
    }
}

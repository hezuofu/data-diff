package io.sketch.datadiff.hash;

import io.sketch.datadiff.core.spi.ChecksumCalculator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for hash functions with runtime extensibility.
 */
public class HashFunctionRegistry {
    
    private static final Map<String, ChecksumCalculator> REGISTRY = new ConcurrentHashMap<>();
    
    static {
        // Register built-in hash functions
        register("crc32", new Crc32Checksum());
        register("md5", new Md5Checksum());
        register("composite", new CompositeChecksum(
            new Crc32Checksum(), 
            new Md5Checksum()
        ));
    }
    
    /**
     * Register a checksum calculator.
     */
    public static void register(String name, ChecksumCalculator calculator) {
        REGISTRY.put(name.toLowerCase(), calculator);
    }
    
    /**
     * Get a checksum calculator by name.
     */
    public static ChecksumCalculator get(String name) {
        ChecksumCalculator calc = REGISTRY.get(name.toLowerCase());
        if (calc == null) {
            throw new IllegalArgumentException("Unknown hash function: " + name);
        }
        return calc;
    }
    
    /**
     * Get the default checksum calculator.
     */
    public static ChecksumCalculator getDefault() {
        return get("md5");
    }
    
    /**
     * Check if a hash function is registered.
     */
    public static boolean isRegistered(String name) {
        return REGISTRY.containsKey(name.toLowerCase());
    }
    
    /**
     * Get all registered hash function names.
     */
    public static java.util.Set<String> getRegisteredNames() {
        return REGISTRY.keySet();
    }
}

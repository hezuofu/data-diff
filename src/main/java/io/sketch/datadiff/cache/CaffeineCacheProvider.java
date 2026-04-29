package io.sketch.datadiff.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

/**
 * Caffeine-based segment checksum cache provider.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class CaffeineCacheProvider {
    
    private final Cache<String, BigInteger> cache;
    
    public CaffeineCacheProvider(int maxSize, long ttlMinutes) {
        this.cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)
            .recordStats()
            .build();
    }
    
    public CaffeineCacheProvider(int maxSize) {
        this(maxSize, 60);
    }
    
    /**
     * Get cached checksum for a segment.
     */
    public BigInteger get(String segmentKey) {
        return cache.getIfPresent(segmentKey);
    }
    
    /**
     * Cache a segment checksum.
     */
    public void put(String segmentKey, BigInteger checksum) {
        cache.put(segmentKey, checksum);
    }
    
    /**
     * Check if cache contains a segment.
     */
    public boolean contains(String segmentKey) {
        return cache.asMap().containsKey(segmentKey);
    }
    
    /**
     * Get cache statistics.
     */
    public String getStats() {
        return cache.stats().toString();
    }
    
    /**
     * Clear all cached entries.
     */
    public void clear() {
        cache.invalidateAll();
    }
    
    /**
     * Get current cache size.
     */
    public long size() {
        return cache.estimatedSize();
    }
}

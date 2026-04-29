package io.sketch.datadiff.cache;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple LRU cache implementation (zero-dependency alternative).
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class LRUCache {
    
    private final Map<String, BigInteger> cache;
    
    public LRUCache(int maxSize) {
        this.cache = new LinkedHashMap<>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, BigInteger> eldest) {
                return size() > maxSize;
            }
        };
    }
    
    public synchronized BigInteger get(String key) {
        return cache.get(key);
    }
    
    public synchronized void put(String key, BigInteger value) {
        cache.put(key, value);
    }
    
    public synchronized boolean contains(String key) {
        return cache.containsKey(key);
    }
    
    public synchronized void clear() {
        cache.clear();
    }
    
    public synchronized int size() {
        return cache.size();
    }
}

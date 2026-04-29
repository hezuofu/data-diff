package io.sketch.datadiff.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backpressure manager for controlling memory usage during streaming.
 * Implements simple threshold-based backpressure.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class BackpressureManager {
    
    private static final Logger log = LoggerFactory.getLogger(BackpressureManager.class);
    
    private final long maxMemoryBytes;
    private final long warningThresholdBytes;
    private long currentMemoryBytes;
    private volatile boolean backpressureActive;
    
    public BackpressureManager(long maxMemoryBytes, long warningThresholdBytes) {
        this.maxMemoryBytes = maxMemoryBytes;
        this.warningThresholdBytes = warningThresholdBytes;
        this.currentMemoryBytes = 0;
        this.backpressureActive = false;
    }
    
    public BackpressureManager(long maxMemoryMB) {
        this(maxMemoryMB * 1024 * 1024, maxMemoryMB * 1024 * 1024 * 8 / 10);
    }
    
    /**
     * Record memory usage increase.
     * 
     * @return true if backpressure is active
     */
    public synchronized boolean recordMemoryIncrease(long bytes) {
        currentMemoryBytes += bytes;
        checkThreshold();
        return backpressureActive;
    }
    
    /**
     * Record memory usage decrease.
     */
    public synchronized void recordMemoryDecrease(long bytes) {
        currentMemoryBytes = Math.max(0, currentMemoryBytes - bytes);
        checkThreshold();
    }
    
    /**
     * Check if should pause processing.
     */
    public boolean shouldPause() {
        return backpressureActive;
    }
    
    /**
     * Wait until backpressure is relieved.
     */
    public void waitForBackpressureRelief() throws InterruptedException {
        while (backpressureActive) {
            Thread.sleep(100);
        }
    }
    
    /**
     * Get current memory usage.
     */
    public synchronized long getCurrentMemoryBytes() {
        return currentMemoryBytes;
    }
    
    /**
     * Get memory usage as percentage of max.
     */
    public double getMemoryUsagePercent() {
        return (double) currentMemoryBytes / maxMemoryBytes * 100.0;
    }
    
    /**
     * Check if backpressure is active.
     */
    public boolean isBackpressureActive() {
        return backpressureActive;
    }
    
    /**
     * Get max memory limit.
     */
    public long getMaxMemoryBytes() {
        return maxMemoryBytes;
    }
    
    /**
     * Check current threshold and update backpressure state.
     */
    private void checkThreshold() {
        boolean wasActive = backpressureActive;
        
        if (currentMemoryBytes >= maxMemoryBytes) {
            backpressureActive = true;
            if (!wasActive) {
                log.warn("Backpressure activated: memory usage {} bytes exceeded limit {} bytes",
                    currentMemoryBytes, maxMemoryBytes);
            }
        } else if (currentMemoryBytes <= warningThresholdBytes) {
            backpressureActive = false;
            if (wasActive) {
                log.info("Backpressure relieved: memory usage decreased to {} bytes", currentMemoryBytes);
            }
        }
    }
    
    /**
     * Reset memory tracking.
     */
    public synchronized void reset() {
        currentMemoryBytes = 0;
        backpressureActive = false;
    }
    
    @Override
    public String toString() {
        return "BackpressureManager[current=%d, max=%d, active=%s, usage=%.1f%%]".formatted(
            currentMemoryBytes,
            maxMemoryBytes,
            backpressureActive,
            getMemoryUsagePercent()
        );
    }
}

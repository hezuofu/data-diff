package io.sketch.datadiff.parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread pool configuration utility.
 * Provides pre-configured thread pools for different scenarios.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class ThreadPoolConfig {
    
    /**
     * Create a fixed thread pool with custom naming.
     */
    public static ExecutorService createFixedThreadPool(int poolSize, String namePrefix) {
        ThreadFactory threadFactory = new NamedThreadFactory(namePrefix);
        return Executors.newFixedThreadPool(poolSize, threadFactory);
    }
    
    /**
     * Create a cached thread pool with custom naming.
     */
    public static ExecutorService createCachedThreadPool(String namePrefix) {
        ThreadFactory threadFactory = new NamedThreadFactory(namePrefix);
        return Executors.newCachedThreadPool(threadFactory);
    }
    
    /**
     * Create a work-stealing pool (Java 8+).
     */
    public static ExecutorService createWorkStealingPool(int parallelism) {
        return Executors.newWorkStealingPool(parallelism);
    }
    
    /**
     * Create a scheduled thread pool.
     */
    public static ExecutorService createScheduledThreadPool(int corePoolSize, String namePrefix) {
        ThreadFactory threadFactory = new NamedThreadFactory(namePrefix);
        return Executors.newScheduledThreadPool(corePoolSize, threadFactory);
    }
    
    /**
     * Gracefully shutdown executor.
     */
    public static void shutdownGracefully(ExecutorService executor, long timeout, TimeUnit unit) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Named thread factory.
     */
    private static class NamedThreadFactory implements ThreadFactory {
        private final String namePrefix;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        
        NamedThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
        }
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + "-thread-" + threadNumber.getAndIncrement());
            t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}

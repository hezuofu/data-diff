package io.sketch.datadiff.stream;

import io.sketch.datadiff.core.model.DiffRecord;

import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Function;

/**
 * Row publisher for reactive streaming of diff results.
 * Implements Flow.Publisher for backpressure support.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class RowPublisher implements Flow.Publisher<DiffRecord>, AutoCloseable {
    
    private final SubmissionPublisher<DiffRecord> publisher;
    
    public RowPublisher() {
        this.publisher = new SubmissionPublisher<>();
    }
    
    public RowPublisher(int maxBufferCapacity) {
        // Use default SubmissionPublisher with specified max buffer
        this.publisher = new SubmissionPublisher<>();
    }
    
    @Override
    public void subscribe(Flow.Subscriber<? super DiffRecord> subscriber) {
        publisher.subscribe(subscriber);
    }
    
    /**
     * Publish a single diff record.
     */
    public void publish(DiffRecord record) {
        publisher.submit(record);
    }
    
    /**
     * Publish multiple diff records.
     */
    public void publishAll(Iterable<DiffRecord> records) {
        for (DiffRecord record : records) {
            publisher.submit(record);
        }
    }
    
    /**
     * Get current subscriber count.
     */
    public int getSubscriberCount() {
        return publisher.getNumberOfSubscribers();
    }
    
    /**
     * Check if there are any subscribers.
     */
    public boolean hasSubscribers() {
        return publisher.getNumberOfSubscribers() > 0;
    }
    
    /**
     * Create a transformed publisher.
     */
    public <T> Flow.Publisher<T> transform(Function<DiffRecord, T> transformer) {
        SubmissionPublisher<T> transformedPublisher = new SubmissionPublisher<>();
        
        publisher.subscribe(new Flow.Subscriber<DiffRecord>() {
            private Flow.Subscription subscription;
            
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscription.request(Long.MAX_VALUE);
            }
            
            @Override
            public void onNext(DiffRecord item) {
                try {
                    T transformed = transformer.apply(item);
                    transformedPublisher.submit(transformed);
                } catch (Exception e) {
                    onError(e);
                }
            }
            
            @Override
            public void onError(Throwable throwable) {
                transformedPublisher.closeExceptionally(throwable);
            }
            
            @Override
            public void onComplete() {
                transformedPublisher.close();
            }
        });
        
        return transformedPublisher;
    }
    
    /**
     * Create a filtered publisher.
     */
    public Flow.Publisher<DiffRecord> filter(java.util.function.Predicate<DiffRecord> predicate) {
        return transform(record -> predicate.test(record) ? record : null);
    }
    
    @Override
    public void close() {
        publisher.close();
    }
    
    /**
     * Get the underlying publisher.
     */
    public SubmissionPublisher<DiffRecord> getPublisher() {
        return publisher;
    }
}

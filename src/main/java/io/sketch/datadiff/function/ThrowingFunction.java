package io.sketch.datadiff.function;

/**
 * Functional interface for functions that may throw checked exceptions.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception> {
    
    R apply(T t) throws E;
    
    /**
     * Wrap a throwing function to handle exceptions.
     */
    static <T, R, E extends Exception> java.util.function.Function<T, R> wrap(
        ThrowingFunction<T, R, E> function
    ) {
        return t -> {
            try {
                return function.apply(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}

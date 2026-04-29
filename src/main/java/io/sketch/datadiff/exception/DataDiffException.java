package io.sketch.datadiff.exception;

/**
 * Base exception for all DataDiff operations.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class DataDiffException extends RuntimeException {
    
    public DataDiffException(String message) {
        super(message);
    }
    
    public DataDiffException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public DataDiffException(Throwable cause) {
        super(cause);
    }
}

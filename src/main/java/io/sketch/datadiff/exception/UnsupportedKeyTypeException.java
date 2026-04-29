package io.sketch.datadiff.exception;

/**
 * Exception thrown when an unsupported key type is encountered.
 */
public class UnsupportedKeyTypeException extends DataDiffException {
    
    public UnsupportedKeyTypeException(String keyType) {
        super("Unsupported primary key type: " + keyType);
    }
    
    public UnsupportedKeyTypeException(String keyType, Throwable cause) {
        super("Unsupported primary key type: " + keyType, cause);
    }
}

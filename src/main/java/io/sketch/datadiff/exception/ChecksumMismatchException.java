package io.sketch.datadiff.exception;

/**
 * Exception thrown when checksum mismatch is detected.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class ChecksumMismatchException extends DataDiffException {
    
    private final String segmentId;
    private final String leftChecksum;
    private final String rightChecksum;
    
    public ChecksumMismatchException(String segmentId, String leftChecksum, String rightChecksum) {
        super("Checksum mismatch in segment %s: left=%s, right=%s".formatted(
            segmentId, leftChecksum, rightChecksum
        ));
        this.segmentId = segmentId;
        this.leftChecksum = leftChecksum;
        this.rightChecksum = rightChecksum;
    }
    
    public String getSegmentId() {
        return segmentId;
    }
    
    public String getLeftChecksum() {
        return leftChecksum;
    }
    
    public String getRightChecksum() {
        return rightChecksum;
    }
}

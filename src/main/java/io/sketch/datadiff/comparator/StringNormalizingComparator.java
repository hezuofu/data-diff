package io.sketch.datadiff.comparator;

/**
 * String normalizing comparator for case-insensitive and whitespace-tolerant comparisons.
 */
public class StringNormalizingComparator {
    
    private final boolean caseInsensitive;
    private final boolean trimWhitespace;
    
    public StringNormalizingComparator(boolean caseInsensitive, boolean trimWhitespace) {
        this.caseInsensitive = caseInsensitive;
        this.trimWhitespace = trimWhitespace;
    }
    
    public StringNormalizingComparator() {
        this(true, true);
    }
    
    /**
     * Compare two strings with normalization.
     */
    public boolean equals(Object left, Object right) {
        if (left == null && right == null) return true;
        if (left == null || right == null) return false;
        
        String leftStr = normalize(left.toString());
        String rightStr = normalize(right.toString());
        
        return leftStr.equals(rightStr);
    }
    
    /**
     * Normalize a string value.
     */
    private String normalize(String value) {
        String result = value;
        if (trimWhitespace) {
            result = result.trim();
        }
        if (caseInsensitive) {
            result = result.toLowerCase();
        }
        return result;
    }
}

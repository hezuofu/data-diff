package io.sketch.datadiff.comparator;

/**
 * Numeric tolerance comparator for floating-point comparisons.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class NumericToleranceComparator {
    
    private final double tolerance;
    
    public NumericToleranceComparator(double tolerance) {
        if (tolerance < 0) {
            throw new IllegalArgumentException("Tolerance must be >= 0");
        }
        this.tolerance = tolerance;
    }
    
    /**
     * Compare two numeric values with tolerance.
     * 
     * @return true if values are within tolerance
     */
    public boolean equals(Object left, Object right) {
        if (left == null && right == null) return true;
        if (left == null || right == null) return false;
        
        double leftNum = ((Number) left).doubleValue();
        double rightNum = ((Number) right).doubleValue();
        
        return Math.abs(leftNum - rightNum) <= tolerance;
    }
    
    /**
     * Get the tolerance value.
     */
    public double getTolerance() {
        return tolerance;
    }
}

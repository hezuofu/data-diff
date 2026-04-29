package io.sketch.datadiff.core.model;

/**
 * Diff type enumeration.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public enum DiffType {
    /** Row exists only in left table */
    LEFT_ONLY,
    /** Row exists only in right table */
    RIGHT_ONLY,
    /** Row exists in both tables but values differ */
    MODIFIED
}

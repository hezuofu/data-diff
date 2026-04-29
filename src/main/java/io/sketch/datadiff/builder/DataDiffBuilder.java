package io.sketch.datadiff.builder;

import io.sketch.datadiff.exception.DataDiffException;
import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.engine.DataDiffEngine;
import io.sketch.datadiff.engine.HashDiffEngine;
import io.sketch.datadiff.engine.JoinDiffEngine;

import javax.sql.DataSource;

/**
 * Builder for DataDiffEngine with fluent API.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class DataDiffBuilder {
    
    private DataSource leftDataSource;
    private DataSource rightDataSource;
    private CompareOptions options;
    
    public DataDiffBuilder leftDataSource(DataSource dataSource) {
        this.leftDataSource = dataSource;
        return this;
    }
    
    public DataDiffBuilder rightDataSource(DataSource dataSource) {
        this.rightDataSource = dataSource;
        return this;
    }
    
    public DataDiffBuilder bothDataSource(DataSource dataSource) {
        this.leftDataSource = dataSource;
        this.rightDataSource = dataSource;
        return this;
    }
    
    public DataDiffBuilder options(CompareOptions options) {
        this.options = options;
        return this;
    }
    
    public DataDiffEngine build() {
        validate();
        
        CompareOptions opts = options != null ? options : CompareOptions.defaults();
        
        var strategy = switch (opts.getAlgorithm()) {
            case HASHDIFF -> new HashDiffEngine();
            case JOINDIFF -> new JoinDiffEngine();
        };
        
        return DataDiffEngine.create(leftDataSource, rightDataSource, strategy);
    }
    
    private void validate() {
        if (leftDataSource == null) {
            throw new DataDiffException("Left data source must be set");
        }
        if (rightDataSource == null) {
            throw new DataDiffException("Right data source must be set");
        }
    }
}

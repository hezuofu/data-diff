package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.DiffResult;
import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.core.strategy.ComparisonStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DataDiffEngineTest {

    @Mock
    private DataSource leftDs;
    @Mock
    private DataSource rightDs;
    @Mock
    private ComparisonStrategy strategy;
    @Mock
    private DiffResult diffResult;
    @Mock
    private TableInfo table;

    @Test
    void testCompareDelegatesToStrategy() {
        DataDiffEngine engine = DataDiffEngine.create(leftDs, rightDs, strategy);
        CompareOptions options = CompareOptions.defaults();
        
        when(strategy.compare(eq(leftDs), eq(rightDs), eq(table), eq(table), any())).thenReturn(diffResult);
        
        DiffResult result = engine.compare(table, table, options);
        
        assertEquals(diffResult, result);
        verify(strategy).compare(eq(leftDs), eq(rightDs), eq(table), eq(table), eq(options));
    }
}

package io.sketch.datadiff.builder;

import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.engine.DataDiffEngine;
import io.sketch.datadiff.exception.DataDiffException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class DataDiffBuilderTest {

    @Mock
    private DataSource dataSource;

    @Test
    void testBuildSuccess() {
        DataDiffEngine engine = new DataDiffBuilder()
            .leftDataSource(dataSource)
            .rightDataSource(dataSource)
            .options(CompareOptions.defaults())
            .build();
        
        assertNotNull(engine);
    }

    @Test
    void testBuildMissingDataSource() {
        DataDiffBuilder builder = new DataDiffBuilder().leftDataSource(dataSource);
        assertThrows(DataDiffException.class, builder::build);
    }

    @Test
    void testBothDataSource() {
        DataDiffEngine engine = new DataDiffBuilder()
            .bothDataSource(dataSource)
            .build();
        assertNotNull(engine);
    }
}

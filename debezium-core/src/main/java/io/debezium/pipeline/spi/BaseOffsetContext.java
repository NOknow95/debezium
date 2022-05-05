package io.debezium.pipeline.spi;

import io.debezium.chunk.TableOffsets;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/14
 */
public abstract class BaseOffsetContext implements OffsetContext {

    private final TableOffsets tableOffsets = new TableOffsets();

    @Override
    public TableOffsets getTableOffsets() {
        return this.tableOffsets;
    }
}

package io.debezium.relational.offset;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public abstract class TableOffset {

    protected final String tableId;

    protected TableOffset(String tableId) {
        this.tableId = tableId;
    }

    public String getTableId() {
        return tableId;
    }

    @Override
    abstract public String toString();
}

package com.nk.event;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public abstract class Off {

    protected final String tableId;

    protected Off(String tableId) {
        this.tableId = tableId;
    }

    public final String getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "Off{" +
                "tableId='" + tableId + '\'' +
                '}';
    }

    public enum Type {
        START, COMPLETE
    }
}

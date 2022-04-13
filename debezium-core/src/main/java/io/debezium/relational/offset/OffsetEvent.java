package io.debezium.relational.offset;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public abstract class OffsetEvent<O extends TableOffset> {

    protected final OffsetEvent.Type type;
    protected final O tableOffset;

    public OffsetEvent(OffsetEvent.Type type, O tableOffset) {
        this.type = type;
        this.tableOffset = tableOffset;
    }

    public Type getType() {
        return type;
    }

    public O getTableOffset() {
        return tableOffset;
    }

    public boolean valid() {
        return type != null && tableOffset != null;
    }

    public enum Type {
        START, COMPLETE
    }

    public abstract OffsetEvent<O> clone(Type type);

    @Override
    public String toString() {
        return "OffsetEvent{" +
                "type=" + type +
                ", tableOffset=" + tableOffset +
                '}';
    }
}

package io.debezium.relational.offset;


/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public interface TableOffsetListener<E extends OffsetEvent> {

    void handle(E event);
}

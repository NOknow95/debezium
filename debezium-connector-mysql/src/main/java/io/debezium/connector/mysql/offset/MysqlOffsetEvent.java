package io.debezium.connector.mysql.offset;

import io.debezium.relational.offset.OffsetEvent;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public class MysqlOffsetEvent extends OffsetEvent<MysqlTableOffset> {

    public MysqlOffsetEvent(Type type, MysqlTableOffset tableOffset) {
        super(type, tableOffset);
    }

    @Override
    public OffsetEvent<MysqlTableOffset> clone(Type type) {
        return new MysqlOffsetEvent(type, this.tableOffset);
    }
}

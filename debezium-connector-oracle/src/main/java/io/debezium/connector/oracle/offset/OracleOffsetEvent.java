package io.debezium.connector.oracle.offset;

import io.debezium.relational.offset.OffsetEvent;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public class OracleOffsetEvent extends OffsetEvent<OracleTableOffset> {

    public OracleOffsetEvent(Type type, OracleTableOffset tableOffset) {
        super(type, tableOffset);
    }

    @Override
    public OffsetEvent<OracleTableOffset> clone(Type type) {
        return new OracleOffsetEvent(type, tableOffset);
    }
}

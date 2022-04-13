package io.debezium.connector.oracle.offset;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.offset.TableOffset;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public class OracleTableOffset extends TableOffset {

    private final Scn scn;

    public OracleTableOffset(String tableId, Scn scn) {
        super(tableId);
        this.scn = scn;
    }

    @Override
    public String toString() {
        return "OracleTableOffset{" +
                "tableId=" + tableId +
                ", scn='" + scn + '\'' +
                '}';
    }
}

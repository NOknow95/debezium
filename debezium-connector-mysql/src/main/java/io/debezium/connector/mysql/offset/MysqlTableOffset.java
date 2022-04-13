package io.debezium.connector.mysql.offset;

import io.debezium.relational.offset.TableOffset;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public class MysqlTableOffset extends TableOffset {

    private final String binlogFileName;
    private final Long binlogPosition;
    private final String gtid;

    public MysqlTableOffset(String tableId, String binlogFileName, Long binlogPosition, String gtid) {
        super(tableId);
        this.binlogFileName = binlogFileName;
        this.binlogPosition = binlogPosition;
        this.gtid = gtid;
    }

    protected MysqlTableOffset(String tableId, String offset) {
        super(tableId);
        this.binlogFileName = null;
        this.binlogPosition = null;
        this.gtid = null;
    }

    @Override
    public String toString() {
        return "MysqlOffset{" +
                "tableId='" + tableId + '\'' +
                ", binlogPosition=" + binlogPosition +
                ", binlogFileName='" + binlogFileName + '\'' +
                ", gtid=" + gtid +
                '}';
    }
}

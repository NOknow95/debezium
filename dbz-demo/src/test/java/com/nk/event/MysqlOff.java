package com.nk.event;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public class MysqlOff extends Off {

    private final String binlogFileName;
    private final long binlogPosition;
    private final String gtid;

    public MysqlOff(String tableId, String binlogFileName, long binlogPosition, String gtid) {
        super(tableId);
        this.binlogFileName = binlogFileName;
        this.binlogPosition = binlogPosition;
        this.gtid = gtid;
    }


    public String getBinlogFileName() {
        return binlogFileName;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public String getGtid() {
        return gtid;
    }

    @Override
    public String toString() {
        return "MysqlOff{" +
                "binlogFileName='" + binlogFileName + '\'' +
                ", binlogPosition=" + binlogPosition +
                ", gtid='" + gtid + '\'' +
                ", tableId='" + tableId + '\'' +
                '}';
    }
}

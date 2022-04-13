package com.nk.event;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public class OracleOff extends Off {

    private final long scn;

    protected OracleOff(String tableId, long scn) {
        super(tableId);
        this.scn = scn;
    }

    public long getScn() {
        return scn;
    }

    @Override
    public String toString() {
        return "OracleOff{" +
                "tableId='" + tableId + '\'' +
                ", scn=" + scn +
                '}';
    }
}

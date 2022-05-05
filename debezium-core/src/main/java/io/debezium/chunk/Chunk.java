package io.debezium.chunk;

import java.io.Serializable;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/21
 */
public class Chunk implements Serializable {

    private static final long serialVersionUID = -6558991093824907760L;

    private String id;
    private String columnName;
    private String begin;
    private String end;
    private String endJoiner;
    private int jdbcType;
    private Boolean completeFlag;
    private long rows;
    private Boolean nullValueFlag;

    public Chunk() {
    }

    public Chunk(String id, String columnName, Boolean nullValueFlag) {
        this.id = id;
        this.columnName = columnName;
        this.nullValueFlag = nullValueFlag;
    }

    public Chunk(String id, String columnName, String begin, String end, int jdbcType, String endJoiner, boolean nullValueFlag) {
        this.id = id;
        this.columnName = columnName;
        this.begin = begin;
        this.end = end;
        this.jdbcType = jdbcType;
        this.endJoiner = endJoiner;
        this.completeFlag = false;
        this.nullValueFlag = nullValueFlag;
    }

    public void complete(long rows) {
        this.completeFlag = true;
        this.rows = rows;
    }

    @Override
    public String toString() {
        return "Chunk{" +
                "id='" + id + '\'' +
                ", columnName='" + columnName + '\'' +
                ", begin='" + begin + '\'' +
                ", end='" + end + '\'' +
                ", endJoiner='" + endJoiner + '\'' +
                ", jdbcType=" + jdbcType +
                ", completeFlag=" + completeFlag +
                ", rows=" + rows +
                ", nullValueFlag=" + nullValueFlag +
                '}';
    }

    // region getter and setter
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getBegin() {
        return begin;
    }

    public void setBegin(String begin) {
        this.begin = begin;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public String getEndJoiner() {
        return endJoiner;
    }

    public void setEndJoiner(String endJoiner) {
        this.endJoiner = endJoiner;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    public Boolean getCompleteFlag() {
        return completeFlag;
    }

    public void setCompleteFlag(Boolean completeFlag) {
        this.completeFlag = completeFlag;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public Boolean getNullValueFlag() {
        return nullValueFlag;
    }

    public void setNullValueFlag(Boolean nullValueFlag) {
        this.nullValueFlag = nullValueFlag;
    }
    //endregion
}

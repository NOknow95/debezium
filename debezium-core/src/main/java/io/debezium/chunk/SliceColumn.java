package io.debezium.chunk;

import java.io.Serializable;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/21
 */
public class SliceColumn implements Serializable {

    private static final long serialVersionUID = -425978482572806915L;

    private String name;
    private String value;
    private int jdbcType;
    private Boolean primaryColFlag;

    public SliceColumn() {
    }

    public SliceColumn(String name, String value, int jdbcType, Boolean primaryColFlag) {
        this.name = name;
        this.value = value;
        this.jdbcType = jdbcType;
        this.primaryColFlag = primaryColFlag;
    }

    //region getter and setter
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    public Boolean getPrimaryColFlag() {
        return primaryColFlag;
    }

    public void setPrimaryColFlag(Boolean primaryColFlag) {
        this.primaryColFlag = primaryColFlag;
    }

    //endregion
}

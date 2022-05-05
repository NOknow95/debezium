package io.debezium.chunk;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/18
 */
public class SelectStatement {

    private final String selectStatement;
    private final boolean last;

    public SelectStatement() {
        this(null, true);
    }

    public SelectStatement(String selectStatement) {
        this(selectStatement, true);
    }

    public SelectStatement(String selectStatement, boolean last) {
        this.selectStatement = selectStatement;
        this.last = last;
    }

    public boolean isPresent() {
        return selectStatement != null && !selectStatement.trim().isEmpty();
    }

    public String get() {
        return selectStatement;
    }

    public boolean isLast() {
        return last;
    }
}

package io.debezium.chunk;

import java.util.List;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/18
 */
public class KeyRange {

    private final List<Object> begin;
    private final List<Object> end;
    private final List<String> endJoiner;
    private final boolean last;

    public KeyRange(List<Object> begin, List<Object> end, boolean last, List<String> endJoiner) {
        this.begin = begin;
        this.end = end;
        this.last = last;
        this.endJoiner = endJoiner;
    }

    public List<Object> getBegin() {
        return begin;
    }

    public List<Object> getEnd() {
        return end;
    }

    public boolean isLast() {
        return last;
    }

    public List<String> getEndJoiner() {
        return endJoiner;
    }
}

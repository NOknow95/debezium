package com.nk.event;

import lombok.Getter;
import lombok.ToString;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
@ToString
@Getter
public abstract class OffEvent<T extends Off> {

    private Off.Type type;
    private T off;

    public OffEvent(Off.Type type, T off) {
        this.type = type;
        this.off = off;
    }
}

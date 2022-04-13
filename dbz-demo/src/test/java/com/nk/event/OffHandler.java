package com.nk.event;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public interface OffHandler<E extends OffEvent> {

    void handle(E event);
}

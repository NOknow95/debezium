package com.nk.event;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public class OracleEvent extends OffEvent<OracleOff> {

    public OracleEvent(Off.Type type, OracleOff off) {
        super(type, off);
    }
}

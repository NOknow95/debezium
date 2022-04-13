package com.nk.event;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
public class MysqlEvent extends OffEvent<MysqlOff>{

    public MysqlEvent(Off.Type type, MysqlOff off) {
        super(type, off);
    }
}

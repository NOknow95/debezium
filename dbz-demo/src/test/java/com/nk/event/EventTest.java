package com.nk.event;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.junit.Test;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
@SuppressWarnings("all")
public class EventTest {

    @Test
    public void test() {
        EventBus eventBus = new EventBus();
        eventBus.register(new OffHandler<MysqlEvent>() {

            @Override
            @Subscribe
            public void handle(MysqlEvent event) {
                System.out.println("m event = " + event);

            }
        });
        eventBus.register(new OffHandler<OracleEvent>() {

            @Override
            @Subscribe
            public void handle(OracleEvent event) {
                System.out.println("o event = " + event);

            }
        });
        OffEvent event1 = new MysqlEvent(Off.Type.START, new MysqlOff("m_table", "fff.binlog", 2, ""));
        OffEvent event2 = new OracleEvent(Off.Type.START, new OracleOff("o_table", 3));

        eventBus.post(event1);

        eventBus.post(event2);
    }

}

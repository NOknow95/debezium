package io.debezium.event;

import com.google.common.eventbus.EventBus;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/13
 */
@SuppressWarnings("UnstableApiUsage")
public final class GlobalEventBus {

    private static final EventBus eventBus = new EventBus();

    public static void register(Object object) {
        eventBus.register(object);
    }

    public static void post(Object event) {
        eventBus.post(event);
    }

    public static void unregister(Object object) {
        eventBus.unregister(object);
    }
}

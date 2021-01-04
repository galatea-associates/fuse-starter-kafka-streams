package org.galatea.kafka.starter.messaging.streams;

@FunctionalInterface
public interface CloseMethod<T> {

    void close(StoreProvider sp, T localState, TaskContext context);
}

package org.galatea.kafka.starter.messaging.streams;

public interface InitMethod<T> {

    void init(StoreProvider sp, T localState, TaskContext context);
}

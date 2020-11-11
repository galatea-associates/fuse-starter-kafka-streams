package org.galatea.kafka.starter.messaging.streams;

@FunctionalInterface
public interface ProcessMethod<K, V, T> {

  void process(K key, V value, StoreProvider sp, TaskContext context, T state);
}

package org.galatea.kafka.starter.messaging.streams.util;

import org.galatea.kafka.starter.messaging.streams.TaskContext;

public interface PeekAction<K,V> {

  void apply(K key, V value, TaskContext context);
}

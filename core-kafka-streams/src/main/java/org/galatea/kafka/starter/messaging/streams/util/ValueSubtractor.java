package org.galatea.kafka.starter.messaging.streams.util;

import org.galatea.kafka.starter.messaging.streams.TaskContext;

public interface ValueSubtractor<K,V> {
  V subtract(K key, V existingValue, V newValue, TaskContext context);
}

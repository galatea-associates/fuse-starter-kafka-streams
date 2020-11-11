package org.galatea.kafka.starter.messaging.streams.util;

import org.galatea.kafka.starter.messaging.streams.TaskContext;

public interface ValueMapper<K, V, V1> {

  V1 apply(K key, V value, TaskContext context);
}

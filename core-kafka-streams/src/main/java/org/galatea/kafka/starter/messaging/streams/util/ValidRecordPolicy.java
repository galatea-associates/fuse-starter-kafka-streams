package org.galatea.kafka.starter.messaging.streams.util;

import org.galatea.kafka.starter.messaging.streams.TaskContext;

public interface ValidRecordPolicy<K, V> {

  boolean shouldKeep(K key, V value, TaskContext context);
}

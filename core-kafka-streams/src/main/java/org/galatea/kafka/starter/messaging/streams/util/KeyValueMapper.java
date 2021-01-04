package org.galatea.kafka.starter.messaging.streams.util;

import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.streams.TaskContext;

public interface KeyValueMapper<K, V, K1, V1> {

  KeyValue<K1, V1> apply(K key, V value, TaskContext context);
}

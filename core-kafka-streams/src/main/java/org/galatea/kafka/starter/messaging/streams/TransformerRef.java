package org.galatea.kafka.starter.messaging.streams;

import org.apache.kafka.streams.KeyValue;

public interface TransformerRef<K, V, K1, V1, T> extends TaskStoreSupplier {

  default T initState() {
    return null;
  }

  default void init(TaskContext<T> context) {
    // do nothing
  }

  KeyValue<K1, V1> transform(K key, V value, TaskContext<T> context);

  default void close(TaskContext<T> context) {
    // do nothing
  }
}

package org.galatea.kafka.starter.messaging.streams;

import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.streams.util.ProcessorForwarder;

@FunctionalInterface
public interface TransformMethod<K, V, K1, V1, T> {

    KeyValue<K1, V1> transform(K key, V value, StoreProvider sp, TaskContext context,
        ProcessorForwarder<K1, V1> forwarder, T state);
}

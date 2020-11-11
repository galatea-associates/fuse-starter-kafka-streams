package org.galatea.kafka.starter.messaging.streams;

import java.util.Optional;
import java.util.function.Consumer;
import org.apache.kafka.streams.KeyValue;

public interface KafkaStreamsStore<K,V> {

  Optional<V> get(K key);

  void all(Consumer<KeyValue<K,V>> consumer);

  void range(Range<K> range, Consumer<KeyValue<K,V>> consumer);
}

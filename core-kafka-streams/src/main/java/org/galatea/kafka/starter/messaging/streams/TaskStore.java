package org.galatea.kafka.starter.messaging.streams;

import java.util.Optional;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TaskStore<K,V> implements KafkaStreamsStore<K,V>{
  private final KeyValueStore<K,V> inner;

  @Override
  public Optional<V> get(K key) {
    return Optional.ofNullable(inner.get(key));
  }

  @Override
  public void all(Consumer<KeyValue<K, V>> consumer) {
    try (KeyValueIterator<K, V> iter = inner.all()) {
      iter.forEachRemaining(consumer);
    }
  }

  @Override
  public void range(Range<K> range, Consumer<KeyValue<K, V>> consumer) {
    try (KeyValueIterator<K, V> iter = inner.range(range.from(), range.to())) {
      iter.forEachRemaining(consumer);
    }
  }

  public void put(K key, V value) {
    inner.put(key, value);
  }

  public void delete(K key) {
    inner.delete(key);
  }
}

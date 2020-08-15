package org.galatea.kafka.starter.messaging.streams;

import java.util.Optional;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.galatea.kafka.starter.messaging.streams.util.RetentionPolicy;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class TaskStore<K, V> implements KafkaStreamsStore<K, V> {

  private final KeyValueStore<K, V> inner;
  private final RetentionPolicy<K, V> retentionPolicy;
  private final TaskContext taskContext;

  @Override
  public Optional<V> get(K key) {
    V raw = inner.get(key);
    if (raw != null && retentionPolicy != null
        && !retentionPolicy.shouldKeep(key, raw, taskContext)) {
      inner.delete(key);
      raw = null;
    }
    return Optional.ofNullable(raw);
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

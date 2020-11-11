package org.galatea.kafka.starter.messaging.streams;

import java.util.Optional;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.galatea.kafka.starter.messaging.streams.util.ValidRecordPolicy;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class TaskStore<K, V> implements KafkaStreamsStore<K, V> {

  private final KeyValueStore<K, V> inner;
  private final ValidRecordPolicy<K, V> validRecordPolicy;
  private final TaskContext taskContext;

  @Override
  public Optional<V> get(K key) {
    V raw = inner.get(key);
    if (raw == null || validRecordPolicy == null || validRecordPolicy
        .shouldKeep(key, raw, taskContext)) {
      return Optional.ofNullable(raw);
    } else {
      inner.delete(key);
      return Optional.empty();
    }
  }

  @Override
  public void all(Consumer<KeyValue<K, V>> consumer) {
    try (KeyValueIterator<K, V> iter = inner.all()) {
      filterIterator(consumer, iter);
    }
  }


  @Override
  public void range(Range<K> range, Consumer<KeyValue<K, V>> consumer) {
    try (KeyValueIterator<K, V> iter = inner.range(range.from(), range.to())) {
      filterIterator(consumer, iter);
    }
  }

  private void filterIterator(Consumer<KeyValue<K, V>> consumer, KeyValueIterator<K, V> iter) {
    iter.forEachRemaining(kv -> {
      if (validRecordPolicy == null || validRecordPolicy
          .shouldKeep(kv.key, kv.value, taskContext)) {
        consumer.accept(kv);
      } else {
        delete(kv.key);
      }
    });
  }

  public void put(K key, V value) {
    if (validRecordPolicy == null || validRecordPolicy.shouldKeep(key, value, taskContext)) {
      inner.put(key, value);
    }
  }

  public void delete(K key) {
    inner.delete(key);
  }
}

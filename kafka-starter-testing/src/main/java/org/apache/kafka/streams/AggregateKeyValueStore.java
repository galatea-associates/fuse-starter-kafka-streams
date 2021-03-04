package org.apache.kafka.streams;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
public class AggregateKeyValueStore<K, V> implements KeyValueStore<K, V> {

  private final Collection<KeyValueStore<K, V>> stores;

  @Override
  public String name() {
    return stores.stream().findFirst().map(KeyValueStore::name).orElse(null);
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean persistent() {
    return stores.stream().findFirst().map(KeyValueStore::persistent).orElse(false);
  }

  @Override
  public boolean isOpen() {
    return stores.stream().findFirst().map(KeyValueStore::isOpen).orElse(false);
  }

  @Override
  public V get(K key) {
    return stores.stream().map(s -> s.get(key)).filter(Objects::nonNull).findFirst().orElse(null);
  }

  @Override
  public KeyValueIterator<K, V> range(Object from, Object to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<K, V> all() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long approximateNumEntries() {
    return stores.stream().mapToLong(KeyValueStore::approximateNumEntries).sum();
  }

  @Override
  public void put(K key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V putIfAbsent(K key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(List<KeyValue<K, V>> entries) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V delete(K key) {
    throw new UnsupportedOperationException();
  }
}

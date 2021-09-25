package org.apache.kafka.streams;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

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

    Queue<KeyValueIterator<K, V>> populatedIterators = stores.stream()
        .map(ReadOnlyKeyValueStore::all)
        .filter(kvKeyValueIterator -> {
          boolean hasNext = kvKeyValueIterator.hasNext();
          if (!hasNext) {
            kvKeyValueIterator.close();
          }
          return hasNext;
        })
        .collect(Collectors.toCollection(LinkedBlockingQueue::new));
    AtomicReference<KeyValueIterator<K, V>> currentIter = new AtomicReference<>(null);
    currentIter.set(populatedIterators.poll());

    Supplier<Optional<KeyValueIterator<K,V>>> currentIterOpt = () -> {
      while (currentIter.get() != null && !currentIter.get().hasNext()) {
        currentIter.get().close();
        currentIter.set(populatedIterators.poll());
      }
      return Optional.ofNullable(currentIter.get());
    };

    return new KeyValueIterator<K, V>() {
      @Override
      public void close() {
        Optional.ofNullable(currentIter.get()).ifPresent(KeyValueIterator::close);
        populatedIterators.forEach(KeyValueIterator::close);
        populatedIterators.clear();
      }

      @Override
      public K peekNextKey() {
        return currentIterOpt.get()
            .map(KeyValueIterator::peekNextKey)
            .orElse(null);
      }

      @Override
      public boolean hasNext() {
        return currentIterOpt.get()
            .map(KeyValueIterator::hasNext)
            .orElse(false);
      }

      @Override
      public KeyValue<K, V> next() {
        return currentIterOpt.get()
            .map(KeyValueIterator::next)
            .orElse(null);
      }
    };
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

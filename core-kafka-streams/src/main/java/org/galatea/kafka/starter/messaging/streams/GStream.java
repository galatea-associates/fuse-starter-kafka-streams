package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.streams.util.KeyValueMapper;
import org.galatea.kafka.starter.messaging.streams.util.PeekAction;
import org.galatea.kafka.starter.messaging.streams.util.RetentionPolicy;
import org.galatea.kafka.starter.messaging.streams.util.ValueMapper;
import org.galatea.kafka.starter.messaging.streams.util.ValueSubtractor;

@Slf4j
@SuppressWarnings("unused")
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class GStream<K, V> {

  private final KStream<K, V> inner;
  private final GStreamBuilder builder;
  private final boolean keyDirty;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private static final AtomicLong peekTransformerCounter = new AtomicLong(0);
  private static final AtomicLong valueTransformerCounter = new AtomicLong(0);
  private static final AtomicLong mapTransformerCounter = new AtomicLong(0);
  private static final AtomicLong deltaCounter = new AtomicLong(0);

  public GStream<K, V> peek(PeekAction<K, V> action) {
    return new GStream<>(inner.transformValues(() -> new PeekTransformer<>(action),
        Named.as("e-peek-" + peekTransformerCounter.incrementAndGet())), builder, keyDirty,
        keySerde, valueSerde);
  }

  public <K1, V1, T> GStream<K1, V1> transform(
      StatefulTransformerRef<K, V, K1, V1, T> transformer) {
    Collection<TaskStoreRef<?, ?>> taskStores = TaskStoreUtil.getTaskStores(transformer);
    createNeededStores(taskStores);

    String[] storeNames = taskStores.stream().map(StoreRef::getName).toArray(String[]::new);
    return new GStream<>(
        inner.transform(() -> new ConfiguredTransformer<>(transformer), storeNames),
        builder, true, null, null);
  }

  public <V1, T> GStream<K, V1> transformValues(
      StatefulTransformerRef<K, V, K, V1, T> transformer) {
    Collection<TaskStoreRef<?, ?>> taskStores = TaskStoreUtil.getTaskStores(transformer);
    createNeededStores(taskStores);

    String[] storeNames = taskStores.stream().map(StoreRef::getName).toArray(String[]::new);
    KStream<K, V1> outStream = inner
        .transform(() -> new ConfiguredTransformer<>(transformer), storeNames);
    return new GStream<>(outStream, builder, keyDirty, keySerde, null);
  }

  public <V1> GStream<K, V1> mapValues(ValueMapper<K, V, V1> mapper) {
    return new GStream<>(inner.transformValues(() -> new ValueMapTransformer<>(mapper),
        Named.as("map-values-" + valueTransformerCounter.incrementAndGet())), builder, keyDirty,
        keySerde, null);
  }

  public <K1, V1> GStream<K1, V1> map(KeyValueMapper<K, V, K1, V1> mapper) {
    return new GStream<>(inner.transform(() -> new KeyValueMapTransformer<>(mapper),
        Named.as("map-" + mapTransformerCounter.incrementAndGet())), builder, true, null, null);
  }

  public GStream<K, V> delta(ValueSubtractor<K, V> subtractor, RetentionPolicy<K, V> retention) {
    return delta(subtractor, retention, null);
  }

  public GStream<K, V> delta(ValueSubtractor<K, V> subtractor, RetentionPolicy<K, V> retention,
      String name) {
    long deltaId = deltaCounter.incrementAndGet();
    String deltaName;
    if (name != null) {
      deltaName = String.format("delta-%d-%s", deltaId, name);
    } else {
      deltaName = String.format("delta-%d", deltaId);
    }

    requireKeySerde();
    requireValueSerde();

    TaskStoreRef<K, V> outerStoreRef = TaskStoreRef.<K, V>builder()
        .name(deltaName)
        .keySerde(keySerde)
        .valueSerde(valueSerde)
        .retentionPolicy(retention)
        .build();

    GStream<K, V> stream = transformValues(new TransformerRef<K, V, K, V>() {
      private final TaskStoreRef<K, V> innerStoreRef = outerStoreRef;

      @Override
      public KeyValue<K, V> transform(K key, V value, ProcessorTaskContext<K, V, Object> context) {
        if (!retention.shouldKeep(key, value, context)) {
          log.info("Delta {} Filtered out record {} | {}", deltaName, key, value);
          return null;
        }
        TaskStore<K, V> store = context.store(innerStoreRef);
        Optional<V> existingValue = store.get(key);
        store.put(key, value);
        return KeyValue.pair(key,
            existingValue.map(e -> subtractor.subtract(key, e, value, context)).orElse(value));
      }
    });
    return new GStream<>(stream.inner, builder, keyDirty, keySerde, valueSerde);
  }

  private void createNeededStores(Collection<TaskStoreRef<?, ?>> storeRefs) {
    storeRefs.forEach(ref -> {
      if (!ref.isCreated()) {
        builder.addStateStore(ref);
        ref.setCreated(true);
      }
    });
  }

  public GStream<K, V> withSerdes(Serde<K> keySerde, Serde<V> valueSerde) {
    return new GStream<>(inner, builder, keyDirty, keySerde, valueSerde);
  }

  private void requireValueSerde() {
    Objects.requireNonNull(valueSerde, String.format("ValueSerde is not defined due to an operator "
            + "that changed value type. Use %s#withSerdes before ValueSerde is required",
        getClass().getSimpleName()));
  }

  private void requireKeySerde() {
    Objects.requireNonNull(keySerde, String.format("KeySerde is not defined due to an operator "
            + "that changed key type. Use %s#withSerdes before KeySerde is required",
        getClass().getSimpleName()));
  }

  public <K1, V1> GStream<K1, V1> asKStream(Function<KStream<K, V>, KStream<K1, V1>> useRawStream) {
    return new GStream<>(useRawStream.apply(inner), builder, keyDirty, null, null);
  }

  public <K1, V1> GStream<K1, V1> mapWithRepartition(Topic<K1, V1> topic,
      KeyValueMapper<K, V, K1, V1> mapper) {
    return map(mapper).repartition(topic);
  }

  public GStream<K, V> repartition(Topic<K, V> topic) {
    KStream<K, V> postRepartition = this
        .peek((k, v, c) -> log.info("{} Producing Repartition [{}|{}] Key: {} Value: {}",
            c.taskId(), className(k), className(v), k, v))
        .inner.through(topic.getName(), producedWith(topic));

    return new GStream<>(postRepartition, builder, false, topic.getKeySerde(),
        topic.getValueSerde())
        .peek((k, v, c) -> log.info("{} Consumed Repartition [{}|{}] Key: {} Value: {}", c.taskId(),
            className(k), className(v), k, v));
  }

  public void to(Topic<K, V> topic) {
    this.peek((k, v, c) -> log.info("{} Produced [{}|{}] Key: {} Value: {}", c.taskId(),
        className(k), className(v), k, v))
        .inner.to(topic.getName(), producedWith(topic));
  }

  private static <K, V> Produced<K, V> producedWith(Topic<K, V> topic) {
    return Produced.with(topic.getKeySerde(), topic.getValueSerde());
  }

  private String className(Object obj) {
    return obj == null ? "N/A" : obj.getClass().getName();
  }

}

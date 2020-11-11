package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.Stores;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.config.StorePersistence;
import org.galatea.kafka.starter.messaging.streams.GStream.StreamState;
import org.galatea.kafka.starter.messaging.streams.util.GStreamConfigs;
import org.galatea.kafka.starter.messaging.streams.util.GStreamConfigs.Property;
import org.galatea.kafka.starter.messaging.streams.util.StoreUpdateCallback;

@Slf4j
@RequiredArgsConstructor
public class GStreamBuilder {

  private final StreamsBuilder inner;
  private final StorePersistenceSupplier persistenceSupplier;
  @Getter
  private final Set<TaskStoreRef<?, ?>> builtTaskStores = new HashSet<>();
  private final Map<String, Object> configuration = new HashMap<>();

  @Getter
  private final Collection<TaskStoreRef<?, ?>> taskStoresWithExpirationPunctuate = new HashSet<>();

  private final Map<DslOperationName, AtomicInteger> operationCounters = new EnumMap<>(
      DslOperationName.class);

  @SuppressWarnings("unchecked")
  <T> T getConfig(Property<T> prop) {
    T value = (T) configuration.get(prop.getKey());
    return Optional.ofNullable(value).orElse(prop.getDefaultValue());
  }

  public void configure(Map<String, Object> propMap) {
    GStreamConfigs.checkValidityOfConfig(propMap);
    configuration.putAll(propMap);
  }

  String getOperationName(DslOperationName opName) {
    return opName.getPrefix() + operationCounters
        .computeIfAbsent(opName, op -> new AtomicInteger(0)).incrementAndGet();
  }

  public <K, V> GStream<K, V> stream(Topic<K, V> topic) {
    StreamState<K, V> newState = StreamState.<K, V>builder()
        .keyDirty(false)
        .keySerde(topic.getKeySerde())
        .valueSerde(topic.getValueSerde())
        .build();
    return new GStream<>(
        inner
            .stream(topic.getName(), Consumed.with(topic.getKeySerde(), topic.getValueSerde())),
        newState, this)
        .peek((k, v, c) -> log
            .info("Consumed [{}|{}] Key: {} Value: {}", className(k), className(v), k, v));
  }

  public <K, V> GStreamBuilder addGlobalStore(GlobalStoreRef<K, V> ref) {
    return addGlobalStore(ref, null);
  }

  public <K, V> GStreamBuilder addGlobalStore(GlobalStoreRef<K, V> ref,
      StoreUpdateCallback<K, V> updateCallback) {
    @NonNull Topic<K, V> topic = ref.getOnTopic();
    StorePersistence persistence = persistenceSupplier.apply(ref.getName());
    log.info("Configuring Global Store '{}' with persistence {}", ref.getName(),
        persistence.name());

    inner.addGlobalStore(
        Stores.keyValueStoreBuilder(persistence.getPersistenceSupplier().apply(ref.getName()),
            ref.getKeySerde(), ref.getValueSerde()),
        topic.getName(), consumedWith(topic), () -> new SimpleProcessor<>(ref, updateCallback));
    return this;
  }

  public Topology build() {
    return inner.build();
  }

  private static <K, V> Consumed<K, V> consumedWith(Topic<K, V> topic) {
    return Consumed.with(topic.getKeySerde(), topic.getValueSerde());
  }

  private String className(Object obj) {
    return obj == null ? "N/A" : obj.getClass().getName();
  }

  <K, V> void addStateStore(TaskStoreRef<K, V> storeRef) {
    StorePersistence persistence = persistenceSupplier.apply(storeRef.getName());
    log.info("Configuring Task Store '{}' with persistence {}", storeRef.getName(),
        persistence.name());
    inner.addStateStore(Stores
        .keyValueStoreBuilder(persistence.getPersistenceSupplier().apply(storeRef.getName()),
            storeRef.getKeySerde(), storeRef.getValueSerde()));
  }
}

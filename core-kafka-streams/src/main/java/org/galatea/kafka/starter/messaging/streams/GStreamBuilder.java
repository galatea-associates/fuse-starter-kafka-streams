package org.galatea.kafka.starter.messaging.streams;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;
import org.galatea.kafka.starter.messaging.Topic;

@Slf4j
public class GStreamBuilder {

  public GStreamBuilder() {
    this.inner = new StreamsBuilder();
  }

  public GStreamBuilder(StreamsBuilder inner) {
    this.inner = inner;
  }

  private final StreamsBuilder inner;

  public <K, V> GStream<K, V> stream(Topic<K, V> topic) {
    return new GStream<>(
        inner.stream(topic.getName(), Consumed.with(topic.getKeySerde(), topic.getValueSerde())),
        this, false, topic.getKeySerde(), topic.getValueSerde())
        .peek((k, v, c) -> log.info("{} Consumed [{}|{}] Key: {} Value: {}", c.taskId(),
            className(k), className(v), k, v));
  }

  public <K, V> GStreamBuilder addGlobalStore(GlobalStoreRef<K, V> ref) {
    @NonNull Topic<K, V> topic = ref.getOnTopic();
    inner.addGlobalStore(
        new KeyValueStoreBuilder<>(new RocksDbKeyValueBytesStoreSupplier(ref.getName(), false),
            ref.getKeySerde(), ref.getValueSerde(), Time.SYSTEM),
        topic.getName(), consumedWith(topic), () -> new SimpleProcessor<>(ref));
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
    log.info("Adding state store {} to inner topology", storeRef.getName());
    inner.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(storeRef.getName()),
            storeRef.getKeySerde(), storeRef.getValueSerde()));
  }
}

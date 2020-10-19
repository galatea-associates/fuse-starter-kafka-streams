package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.streams.util.KeyValueMapper;
import org.galatea.kafka.starter.messaging.streams.util.PeekAction;
import org.galatea.kafka.starter.messaging.streams.util.ValueMapper;

@Slf4j
@SuppressWarnings("unused")
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class GStream<K, V> {

  // TODO:
  //  TransformerRef should be interface
  //  punctuates are a collection
  //  stateStores are a collection
  //  TaskStoreRef configurable for in-memory or persistent, based on yml
  //  Tests use in-memory
  //  replace all DSL functions with GStream methods
  //  anything that contains K,V should implement KeyValueType<K,V> interface
  //  KafkaClusterManager to create and maintain topics
  //  module mode that does a streams reset instead of starting up streams
  //  replace GStreamInterceptor with GPartitioner, which is configured for the streams producer
  //  make core-kafka-streams spring-agnostic - make new core-kafka-streams-spring that adds spring

  @Value
  @Builder(toBuilder = true)
  static class StreamState<K1, V1> {

    boolean keyDirty;
    Serde<K1> keySerde;
    Serde<V1> valueSerde;
  }

  private final KStream<K, V> inner;
  private final StreamState<K, V> state;
  private final GStreamBuilder builder;
  private static final AtomicLong peekTransformerCounter = new AtomicLong(0);
  private static final AtomicLong valueTransformerCounter = new AtomicLong(0);
  private static final AtomicLong mapTransformerCounter = new AtomicLong(0);
  private static final AtomicLong deltaCounter = new AtomicLong(0);

  public GStream<K, V> peek(PeekAction<K, V> action) {

    // construct new GStream to retain the current StreamState, since this won't cause any updates
    // to the state (but the transformValues by itself would update the state)
    return new GStream<>(transformValues(new PeekTransformerRef<>(action)).inner, state, builder);
  }

  public GStream<K, V> repartitionWith(Function<K, String> keyExtractor, Topic<K, V> topic) {
    return transform(new PartitionKeyInjectorTransformerRef<>(keyExtractor))
        .repartition(topic);
  }

  public <K1, V1, T> GStream<K1, V1> transform(
      StatefulTransformerRef<K, V, K1, V1, T> transformer) {
    Collection<TaskStoreRef<?, ?>> taskStores = TaskStoreUtil.getTaskStores(transformer);
    createNeededStores(taskStores);

    String[] storeNames = taskStores.stream().map(StoreRef::getName).toArray(String[]::new);
    StreamState<K1, V1> newState = StreamState.<K1, V1>builder().keyDirty(true).build();
    return new GStream<>(
        inner.transform(() -> new ConfiguredTransformer<>(transformer), storeNames), newState,
        builder);
  }

  public <V1, T> GStream<K, V1> transformValues(
      StatefulTransformerRef<K, V, K, V1, T> transformer) {
    Collection<TaskStoreRef<?, ?>> taskStores = TaskStoreUtil.getTaskStores(transformer);
    createNeededStores(taskStores);

    String[] storeNames = taskStores.stream().map(StoreRef::getName).toArray(String[]::new);
    String named = transformer.named();
    KStream<K, V1> outStream;
    if (named != null) {
      outStream = inner
          .transform(() -> new ConfiguredTransformer<>(transformer), Named.as(named), storeNames);
    } else {
      outStream = inner.transform(() -> new ConfiguredTransformer<>(transformer), storeNames);
    }
    StreamState<K, V1> newState = StreamState.<K, V1>builder()
        .keySerde(state.getKeySerde())
        .keyDirty(state.isKeyDirty())
        .build();
    return new GStream<>(outStream, newState, builder);
  }

  public <V1> GStream<K, V1> mapValues(ValueMapper<K, V, V1> mapper) {
    StreamState<K, V1> newState = StreamState.<K, V1>builder()
        .keyDirty(state.isKeyDirty())
        .keySerde(state.getKeySerde())
        .build();
    return new GStream<>(inner.transformValues(() -> new ValueMapTransformer<>(mapper),
        Named.as("map-values-" + valueTransformerCounter.incrementAndGet())), newState, builder);
  }

  public <K1, V1> GStream<K1, V1> map(KeyValueMapper<K, V, K1, V1> mapper) {
    StreamState<K1, V1> newState = StreamState.<K1, V1>builder()
        .keyDirty(true)
        .build();
    return new GStream<>(inner.transform(() -> new KeyValueMapTransformer<>(mapper),
        Named.as("map-" + mapTransformerCounter.incrementAndGet())), newState, builder);
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
    StreamState<K, V> newState = StreamState.<K, V>builder()
        .keyDirty(state.isKeyDirty())
        .keySerde(keySerde)
        .valueSerde(valueSerde)
        .build();
    return new GStream<>(inner, newState, builder);
  }

  private void requireValueSerde() {
    Objects.requireNonNull(state.getValueSerde(),
        String.format("ValueSerde is not defined due to an operator "
                + "that changed value type. Use %s#withSerdes before ValueSerde is required",
            getClass().getSimpleName()));
  }

  private void requireKeySerde() {
    Objects.requireNonNull(state.getKeySerde(),
        String.format("KeySerde is not defined due to an operator "
                + "that changed key type. Use %s#withSerdes before KeySerde is required",
            getClass().getSimpleName()));
  }

  public <K1, V1> GStream<K1, V1> asKStream(Function<KStream<K, V>, KStream<K1, V1>> useRawStream) {
    StreamState<K1, V1> newState = StreamState.<K1, V1>builder()
        .keyDirty(true)
        .build();
    return new GStream<>(useRawStream.apply(inner), newState, builder);
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

    StreamState<K, V> newState = StreamState.<K, V>builder()
        .keyDirty(false)
        .keySerde(topic.getKeySerde())
        .valueSerde(topic.getValueSerde())
        .build();
    return new GStream<>(postRepartition, newState, builder)
        .peek((k, v, c) -> log
            .info("{} Consumed Repartition [{}|{}] Key: {} Value: {}", c.taskId(), className(k),
                className(v), k, v));
  }

  public void to(Topic<K, V> topic) {
    this.peek((k, v, c) -> log
        .info("Produced [{}|{}] Key: {} Value: {}", className(k), className(v), k, v))
        .inner.to(topic.getName(), producedWith(topic));
  }

  private static <K, V> Produced<K, V> producedWith(Topic<K, V> topic) {
    return Produced.with(topic.getKeySerde(), topic.getValueSerde());
  }

  private String className(Object obj) {
    return obj == null ? "N/A" : obj.getClass().getName();
  }

}

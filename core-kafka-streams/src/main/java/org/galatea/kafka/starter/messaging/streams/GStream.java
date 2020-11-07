package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.streams.domain.ConfiguredHeaders;
import org.galatea.kafka.starter.messaging.streams.util.KeyValueMapper;
import org.galatea.kafka.starter.messaging.streams.util.PeekAction;
import org.galatea.kafka.starter.messaging.streams.util.ValueMapper;

@Slf4j
@SuppressWarnings("unused")
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class GStream<K, V> {

  // TODO:
  //  replace all DSL functions with GStream methods
  //  KafkaClusterManager to create and maintain topics
  //  module mode that does a streams reset instead of starting up streams
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

  public GStream<K, V> peek(PeekAction<K, V> action) {

    GStream<K, V> transformedStream = transform(
        TransformerTemplate.<K, V, K, V, Object>builder()
            .transformMethod((key, value, sp, context, forwarder, state) -> {
              action.apply(key, value, context);
              return KeyValue.pair(key, value);
            })
            .build());
    // construct new GStream to retain the current StreamState, since this won't cause any updates
    // to the state (but the transformValues by itself would update the state)
    return new GStream<>(transformedStream.inner, state, builder);
  }

  public GStream<K, V> repartitionWith(Function<K, byte[]> keyExtractor, Topic<K, V> topic) {
    return transform(TransformerTemplate.<K, V, K, V, Object>builder()
        .transformMethod((key, value, sp, context, forwarder, state) -> {
          byte[] partitionKey = keyExtractor.apply(key);
          Headers headers = context.headers();
          headers.remove(ConfiguredHeaders.NEW_PARTITION_KEY.getKey());
          headers.add(ConfiguredHeaders.NEW_PARTITION_KEY.getKey(), partitionKey);
          return KeyValue.pair(key, value);
        })
        .build());
  }

  public <K1, V1, T> GStream<K1, V1> transform(
      TransformerTemplate<K, V, K1, V1, T> transformer) {

    Collection<TaskStoreRef<?, ?>> taskStores = transformer.getTaskStores();
    createNeededStores(taskStores);

    String[] storeNames = taskStores.stream().map(StoreRef::getName)
        .toArray(String[]::new);
    StreamState<K1, V1> newState = StreamState.<K1, V1>builder().keyDirty(true).build();

    KStream<K1, V1> transformed;
    if (transformer.getName() != null) {
      Named named = Named.as(transformer.getName());
      transformed = inner
          .transform(() -> new FromTemplateTransformer<>(transformer), named, storeNames);
    } else {
      transformed = inner
          .transform(() -> new FromTemplateTransformer<>(transformer), storeNames);
    }
    return new GStream<>(transformed, newState, builder);
  }

  public <T> void process(ProcessorTemplate<K, V, T> processorTemplate) {

    Collection<TaskStoreRef<?, ?>> taskStores = processorTemplate.getTaskStores();
    createNeededStores(taskStores);

    String[] storeNames = taskStores.stream().map(StoreRef::getName)
        .toArray(String[]::new);

    if (processorTemplate.getName() != null) {
      Named named = Named.as(processorTemplate.getName());
      inner.process(() -> new FromTemplateProcessor<>(processorTemplate), named, storeNames);
    } else {
      inner.process(() -> new FromTemplateProcessor<>(processorTemplate), storeNames);
    }
  }

  public <V1> GStream<K, V1> mapValues(ValueMapper<K, V, V1> mapper) {
    StreamState<K, V1> newState = StreamState.<K, V1>builder()
        .keyDirty(state.isKeyDirty())
        .keySerde(state.getKeySerde())
        .build();

    GStream<K, V1> transformed = transform(TransformerTemplate.<K, V, K, V1, Object>builder()
        .transformMethod((key, value, sp, context, forwarder, state) -> KeyValue
            .pair(key, mapper.apply(key, value, context)))
        .name(builder.getOperationName(DslOperationName.MAP_VALUE))
        .build());

    return new GStream<>(transformed.inner, newState, builder);
  }

  public <K1, V1> GStream<K1, V1> map(KeyValueMapper<K, V, K1, V1> mapper) {
    StreamState<K1, V1> newState = StreamState.<K1, V1>builder()
        .keyDirty(true)
        .build();
    GStream<K1, V1> transformed = transform(TransformerTemplate.<K, V, K1, V1, Object>builder()
        .transformMethod(
            (key, value, sp, context, forwarder, state) -> mapper.apply(key, value, context))
        .name(builder.getOperationName(DslOperationName.MAP))
        .build());

    return new GStream<>(transformed.inner, newState, builder);
  }

  private void createNeededStores(Collection<TaskStoreRef<?, ?>> storeRefs) {
    storeRefs.forEach(ref -> {
      if (builder.getBuiltTaskStores().add(ref)) {
        log.info("Adding State store to topology: {}", ref.getName());
        builder.addStateStore(ref);
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

  public <K1, V1> GStream<K1, V1> asKStream(
      Function<KStream<K, V>, KStream<K1, V1>> useRawStream) {
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
            .info("Consumed Repartition [{}|{}] Key: {} Value: {}", className(k), className(v), k,
                v));
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

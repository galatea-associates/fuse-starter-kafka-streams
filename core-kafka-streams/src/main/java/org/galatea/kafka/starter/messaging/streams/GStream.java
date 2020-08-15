package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.streams.util.PeekAction;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class GStream<K, V> {

  private final KStream<K, V> inner;
  private static final AtomicLong peekTransformerCounter = new AtomicLong(0);

  public GStream<K, V> peek(PeekAction<K, V> action) {
    inner.transformValues(() -> new PeekTransformer<>(action),
        Named.as("e-peek-" + peekTransformerCounter.incrementAndGet()));
    return this;
  }

  public <K1, V1, T> GStream<K1, V1> transform(TransformerRef<K, V, K1, V1, T> transformer) {
    Collection<TaskStoreRef<?, ?>> refs = Optional.ofNullable(transformer.taskStores())
        .orElse(Collections.emptyList());
    String[] storeNames = refs.stream().map(StoreRef::getName).toArray(String[]::new);
    return new GStream<>(
        inner.transform(() -> new ConfiguredTransformer<>(transformer), storeNames));
  }

  public GStream<K, V> repartition(Topic<K, V> topic) {
    KStream<K, V> postRepartition = this
        .peek((k, v, c) -> log.info("{} Producing Repartition [{}|{}] Key: {} Value: {}",
            c.taskId(), className(k), className(v), k, v))
        .inner.through(topic.getName(), producedWith(topic));

    return new GStream<>(postRepartition)
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

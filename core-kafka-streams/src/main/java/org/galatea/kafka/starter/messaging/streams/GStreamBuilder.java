package org.galatea.kafka.starter.messaging.streams;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.galatea.kafka.starter.messaging.Topic;

@Slf4j
@RequiredArgsConstructor
public class GStreamBuilder {

  private final StreamsBuilder inner;

  public <K, V> GStream<K, V> stream(Topic<K, V> topic) {
    return new GStream<>(
        inner.stream(topic.getName(), Consumed.with(topic.getKeySerde(), topic.getValueSerde())))
        .peek((k, v, c) -> log.info("{} Consumed [{}|{}] Key: {} Value: {}", c.taskId(),
            className(k), className(v), k, v));
  }

  public <K, V> GStreamBuilder addGlobalStore(GlobalStoreRef<K, V> ref) {
    @NonNull Topic<K, V> topic = ref.getOnTopic();
    inner.globalTable(topic.getName(), Consumed.with(topic.getKeySerde(), topic.getValueSerde()),
        Materialized.as(ref.getName()));
    return this;
  }

  private String className(Object obj) {
    return obj == null ? "N/A" : obj.getClass().getName();
  }
}

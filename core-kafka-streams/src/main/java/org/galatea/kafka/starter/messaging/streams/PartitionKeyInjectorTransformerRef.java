package org.galatea.kafka.starter.messaging.streams;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.streams.domain.ConfiguredHeaders;

@RequiredArgsConstructor
public class PartitionKeyInjectorTransformerRef<K, V> extends TransformerRef<K, V, K, V> {

  private final Function<K, String> keyExtractor;

  @Override
  public KeyValue<K, V> transform(K key, V value, ProcessorTaskContext<K, V, Object> context) {
    String partitionKey = keyExtractor.apply(key);
    Headers headers = context.headers();
    headers.remove(ConfiguredHeaders.NEW_PARTITION_KEY.getKey());
    headers.add(ConfiguredHeaders.NEW_PARTITION_KEY.getKey(), partitionKey.getBytes());
    return KeyValue.pair(key, value);
  }
}

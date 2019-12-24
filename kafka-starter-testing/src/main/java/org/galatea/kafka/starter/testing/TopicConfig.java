package org.galatea.kafka.starter.testing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

@RequiredArgsConstructor
@Getter
public class TopicConfig<K, V> {

  private final String topicName;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final Callable<K> createEmptyKey;
  private final Callable<V> createEmptyValue;
  private final Map<String, String> aliases = new HashMap<>();
  private final Map<String, Function<String, Object>> conversions = new HashMap<>();
  private final Map<String, String> defaultValues = new HashMap<>();
  private ConsumerRecordFactory<K, V> factory = null;

  public void registerConversion(String onField, Function<String, Object> conversion) {
    conversions.put(onField, conversion);
  }

  public void registerAlias(String alias, String expandedFieldName) {
    aliases.put(alias, expandedFieldName);
  }

  public K createKey() throws Exception {
    return createEmptyKey.call();
  }

  public V createValue() throws Exception {
    return createEmptyValue.call();
  }

  public ConsumerRecordFactory<K, V> factory() {
    if (factory == null) {
      factory = new ConsumerRecordFactory<>(topicName, keySerde.serializer(),
          valueSerde.serializer());
    }
    return factory;
  }
}

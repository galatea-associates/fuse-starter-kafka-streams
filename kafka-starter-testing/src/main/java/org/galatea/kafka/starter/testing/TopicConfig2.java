package org.galatea.kafka.starter.testing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;

@Getter(AccessLevel.PACKAGE)
public class TopicConfig2<K, V> {

  private final String topicName;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final Callable<K> createEmptyKey;
  private final Callable<V> createEmptyValue;
  private final Map<String, String> aliases = new HashMap<>();
  private final Map<String, Function<String, Object>> conversions = new HashMap<>();
  private final Map<String, String> defaultValues = new HashMap<>();

  TopicConfig2(String topicName, Serde<K> keySerde, Serde<V> valueSerde,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue) {
    this.topicName = topicName;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.createEmptyKey = createEmptyKey;
    this.createEmptyValue = createEmptyValue;
  }

  public TopicConfig2<K, V> registerConversion(String onField, Function<String, Object> conversion) {
    conversions.put(onField, conversion);
    return this;
  }

  public TopicConfig2<K, V> registerAlias(String alias, String expandedFieldName) {
    aliases.put(alias, expandedFieldName);
    return this;
  }

  public TopicConfig2<K, V> registerDefault(String onField, String value) {
    defaultValues.put(onField, value);
    return this;
  }

  K createKey() throws Exception {
    return createEmptyKey.call();
  }

  V createValue() throws Exception {
    return createEmptyValue.call();
  }
}

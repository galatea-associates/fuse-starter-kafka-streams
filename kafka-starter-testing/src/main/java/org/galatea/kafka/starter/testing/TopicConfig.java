package org.galatea.kafka.starter.testing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

@Getter(AccessLevel.PACKAGE)
public class TopicConfig<K, V> {

  private final String topicName;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final Callable<K> createEmptyKey;
  private final Callable<V> createEmptyValue;
  private final TestInputTopic<K,V> configuredInput;
  private final TestOutputTopic<K,V> configuredOutput;
  private final Map<String, String> aliases = new HashMap<>();
  private final Map<String, Function<String, Object>> conversions = new HashMap<>();
  private final Map<String, String> defaultValues = new HashMap<>();

  TopicConfig(String topicName, Serde<K> keySerde, Serde<V> valueSerde,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue, TestInputTopic<K,V> configuredInput) {
    this.topicName = topicName;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.createEmptyKey = createEmptyKey;
    this.createEmptyValue = createEmptyValue;
    this.configuredInput = configuredInput;
    this.configuredOutput = null;
  }

  TopicConfig(String topicName, Serde<K> keySerde, Serde<V> valueSerde,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue) {
    this.topicName = topicName;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.createEmptyKey = createEmptyKey;
    this.createEmptyValue = createEmptyValue;
    this.configuredInput = null;
    this.configuredOutput = null;
  }

  TopicConfig(String topicName, Serde<K> keySerde, Serde<V> valueSerde,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue, TestOutputTopic<K,V> configuredOutput) {
    this.topicName = topicName;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.createEmptyKey = createEmptyKey;
    this.createEmptyValue = createEmptyValue;
    this.configuredInput = null;
    this.configuredOutput = configuredOutput;
  }

  public TopicConfig<K, V> registerConversion(String onField, Function<String, Object> conversion) {
    conversions.put(onField, conversion);
    return this;
  }

  public TopicConfig<K, V> registerAlias(String alias, String expandedFieldName) {
    aliases.put(alias, expandedFieldName);
    return this;
  }

  public TopicConfig<K, V> registerDefault(String onField, String value) {
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

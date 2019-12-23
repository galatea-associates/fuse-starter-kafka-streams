package org.galatea.kafka.starter.testing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TopicConfig<K,V> {
  private final Callable<K> createEmptyKey;
  private final Callable<V> createEmptyValue;
  @Getter
  private final Map<String, String> aliases = new HashMap<>();
  @Getter
  private final Map<String, Function<String, Object>> conversions = new HashMap<>();

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
}

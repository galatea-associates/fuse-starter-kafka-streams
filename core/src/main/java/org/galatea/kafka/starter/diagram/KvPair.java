package org.galatea.kafka.starter.diagram;

import lombok.Value;

@Value
public class KvPair {
  Object key;
  Object value;

  public KvPair withValue(Object value) {
    return new KvPair(key, value);
  }

  public String toString() {
    return String.format("{%s, %s}", key, value);
  }
}

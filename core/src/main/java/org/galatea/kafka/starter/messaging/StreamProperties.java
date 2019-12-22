package org.galatea.kafka.starter.messaging;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;

public class StreamProperties {

  @Getter
  private Map<String, String> streams = new HashMap<>();

  public Properties asProperties() {
    Properties props = new Properties();
    streams.forEach(props::put);
    return props;
  }

  public Map<String, Object> asObjectMap() {
    Map<String, Object> outputMap = new HashMap<>();
    streams.forEach(outputMap::put);
    return outputMap;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    streams.forEach((key, value) -> sb.append("  ").append(key).append(": ").append(value)
        .append("\n"));
    return sb.toString();
  }
}
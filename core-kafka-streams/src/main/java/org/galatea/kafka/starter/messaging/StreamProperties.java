package org.galatea.kafka.starter.messaging;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties
public class StreamProperties {

  @Getter
  private final Map<String, String> kafkaStreams = new HashMap<>();

  public Properties asProperties() {
    Properties props = new Properties();
    kafkaStreams.forEach(props::put);
    return props;
  }

  public Map<String, Object> asObjectMap() {
    Map<String, Object> outputMap = new HashMap<>();
    kafkaStreams.forEach(outputMap::put);
    return outputMap;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    kafkaStreams.forEach((key, value) -> sb.append("  ").append(key).append(": ").append(value)
        .append("\n"));
    return sb.toString();
  }
}
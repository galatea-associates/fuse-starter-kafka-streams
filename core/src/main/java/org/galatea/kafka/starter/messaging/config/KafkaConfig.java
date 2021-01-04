package org.galatea.kafka.starter.messaging.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.RequiredArgsConstructor;

@SuppressWarnings("unused")
@RequiredArgsConstructor
public class KafkaConfig {

  private final InternalKafkaConfig inner;

  public Map<String, Object> streamsMap() {
    Map<String, Object> propMap = new HashMap<>();
    propMap.putAll(inner.getGlobal());
    propMap.putAll(inner.getStreams());

    return propMap;
  }

  public Properties streamsProperties() {
    Properties props = new Properties();
    props.putAll(streamsMap());
    return props;
  }

  public Map<String, Object> consumerMap() {
    Map<String, Object> propMap = new HashMap<>();
    propMap.putAll(inner.getGlobal());
    propMap.putAll(inner.getConsumer());

    return propMap;
  }

  public Properties consumerProperties() {
    Properties props = new Properties();
    props.putAll(consumerMap());
    return props;
  }

  public Map<String, Object> producerMap() {
    Map<String, Object> propMap = new HashMap<>();
    propMap.putAll(inner.getGlobal());
    propMap.putAll(inner.getProducer());

    return propMap;
  }

  public Properties producerProperties() {
    Properties props = new Properties();
    props.putAll(producerMap());
    return props;
  }

  public Map<String, Object> adminMap() {
    Map<String, Object> propMap = new HashMap<>();
    propMap.putAll(inner.getGlobal());
    propMap.putAll(inner.getAdmin());

    return propMap;
  }

  public Properties adminProperties() {
    Properties props = new Properties();
    props.putAll(adminMap());
    return props;
  }

}

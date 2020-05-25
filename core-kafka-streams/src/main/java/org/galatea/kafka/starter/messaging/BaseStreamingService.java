package org.galatea.kafka.starter.messaging;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.galatea.kafka.starter.service.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

@Slf4j
@Import({StreamProperties.class, KafkaStreamsStarter.class})
public abstract class BaseStreamingService implements Service {

  @Autowired
  private StreamProperties streamProperties;

  protected abstract Topology buildTopology();

  @Getter(AccessLevel.PROTECTED)
  private KafkaStreams streams;

  @Override
  final public void start() {
    streams = new KafkaStreams(buildTopology(), streamProperties.asProperties());
    onStreamCreation(streams);
    streams.start();
  }

  @Override
  final public void stop() {
    streams.close();
  }

  /**
   * Override this method in order to affect the kafkaStreams object immediately after creation,
   * before start
   */
  protected void onStreamCreation(KafkaStreams streams) {
    // do nothing by default
  }

  public void logConsume(Object key, Object value) {
    log.info("Consumed [{}|{}]: {} | {} ", classNameDisplay(key), classNameDisplay(value), key,
        value);
  }

  public void logProduce(Object key, Object value) {
    log.info("Produced [{}|{}]: {} | {} ", classNameDisplay(key), classNameDisplay(value), key,
        value);
  }

  private String classNameDisplay(Object obj) {
    return obj == null ? "null" : obj.getClass().getSimpleName();
  }
}

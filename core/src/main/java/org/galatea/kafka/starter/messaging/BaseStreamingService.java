package org.galatea.kafka.starter.messaging;

import java.util.Properties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.galatea.kafka.starter.service.Service;

@Slf4j
public abstract class BaseStreamingService implements Service {

  protected abstract Topology buildTopology();

  protected Properties streamProperties = null;
  @Getter(AccessLevel.PROTECTED)
  private KafkaStreams streams;

  @Override
  final public void start() {
    if (streamProperties == null) {
      throw new IllegalStateException(
          "Stream Properties must be set before trying to start streams");
    }
    streams = new KafkaStreams(buildTopology(), streamProperties);
    streams.start();
  }

  @Override
  final public void stop() {
    streams.close();
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

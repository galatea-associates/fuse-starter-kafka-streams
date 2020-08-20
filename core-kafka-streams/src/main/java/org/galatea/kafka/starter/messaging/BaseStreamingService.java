package org.galatea.kafka.starter.messaging;

import java.io.Closeable;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.galatea.kafka.starter.messaging.streams.GStreamBuilder;
import org.galatea.kafka.starter.messaging.streams.TopologyProvider;
import org.galatea.kafka.starter.service.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Slf4j
@Import({KafkaStreamsConfig.class, KafkaStreamsStarter.class})
@Component
@ConditionalOnBean(TopologyProvider.class)
public class BaseStreamingService implements Service, Closeable {

  @Autowired
  private KafkaStreamsConfig kafkaStreamsConfig;
  @Autowired
  private TopologyProvider topologyProvider;

  @Getter(AccessLevel.PROTECTED)
  private KafkaStreams streams;

  @Override
  final public void start() {
    Topology topology = topologyProvider.buildTopology(new GStreamBuilder(new StreamsBuilder()));
    log.info("{}", topology.describe());
    streams = new KafkaStreams(topology, kafkaStreamsConfig.asProperties());
    streams.start();
  }

  @Override
  final public void stop() {
    streams.close();
  }

  @Override
  public void close() throws IOException {
    stop();
  }
}

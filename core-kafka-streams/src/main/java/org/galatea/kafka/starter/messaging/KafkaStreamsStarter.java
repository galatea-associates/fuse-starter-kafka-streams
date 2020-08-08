package org.galatea.kafka.starter.messaging;

import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaStreamsStarter {

  private final BaseStreamingService service;

  @PostConstruct
  public void start() {
    log.info("Starting Kafka Streams");
    service.start();
  }
}

package org.galatea.kafka.starter.messaging.streams;

import org.galatea.kafka.starter.messaging.streams.domain.SubKeyAndValue;

public class SubKeyedStream<K, K1, V> {

  private final GStreamBuilder builder;

  SubKeyedStream(GStream<K, SubKeyAndValue<K1, V>> stream, GStreamBuilder builder) {
    this.builder = builder;
  }

  // TODO: add
  // TODO: add delta
  // TODO: add aggregate

}

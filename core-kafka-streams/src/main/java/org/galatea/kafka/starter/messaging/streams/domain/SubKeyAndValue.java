package org.galatea.kafka.starter.messaging.streams.domain;

import lombok.Value;

@Value
public class SubKeyAndValue<K,V> {
  K key;
  V value;
}

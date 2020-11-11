package org.galatea.kafka.starter.messaging;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@RequiredArgsConstructor
@Value
public class Topic<K,V> implements SerdePairSupplier<K,V> {

  String name;
  Serde<K> keySerde;
  Serde<V> valueSerde;
}

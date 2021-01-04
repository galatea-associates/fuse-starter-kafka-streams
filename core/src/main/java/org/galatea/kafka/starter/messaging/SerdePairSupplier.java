package org.galatea.kafka.starter.messaging;

import org.apache.kafka.common.serialization.Serde;

public interface SerdePairSupplier<K,V> {

  Serde<K> getKeySerde();
  Serde<V> getValueSerde();
}

package org.galatea.kafka.starter.messaging.streams;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.starter.messaging.SerdePairSupplier;

@Getter
@RequiredArgsConstructor
class StoreRef<K, V> implements SerdePairSupplier<K,V> {

  @NonNull
  private final String name;
  @NonNull
  private final Serde<K> keySerde;
  @NonNull
  private final Serde<V> valueSerde;
}

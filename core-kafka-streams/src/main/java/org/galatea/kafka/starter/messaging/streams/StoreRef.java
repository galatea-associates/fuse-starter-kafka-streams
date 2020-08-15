package org.galatea.kafka.starter.messaging.streams;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;

@Getter
@RequiredArgsConstructor
class StoreRef<K, V> {

  @NonNull
  private final String name;
  @NonNull
  private final Serde<K> keySerde;
  @NonNull
  private final Serde<V> valueSerde;
}

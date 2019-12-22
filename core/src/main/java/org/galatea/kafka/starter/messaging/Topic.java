package org.galatea.kafka.starter.messaging;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

@RequiredArgsConstructor
@Value
public class Topic<K,V> {

  String name;
  Serde<K> keySerde;
  Serde<V> valueSerde;

  public Consumed<K,V> consumedWith() {
    return Consumed.with(keySerde, valueSerde);
  }

  public Produced<K, V> producedWith() {
    return Produced.with(keySerde, valueSerde);
  }
}

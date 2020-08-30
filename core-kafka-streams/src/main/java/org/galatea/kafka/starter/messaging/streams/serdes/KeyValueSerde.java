package org.galatea.kafka.starter.messaging.streams.serdes;

import com.apple.foundationdb.tuple.Tuple;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

public class KeyValueSerde<K, V> implements Serde<KeyValue<K, V>> {

  private final Serializer<KeyValue<K, V>> serializer;
  private final Deserializer<KeyValue<K, V>> deserializer;
  @Getter
  private final Serde<K> keySerde;
  @Getter
  private final Serde<V> valueSerde;

  public KeyValueSerde(Serde<K> keySerde, Serde<V> valueSerde) {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    serializer = (s, data) -> {
      byte[] outKey = keySerde.serializer().serialize(keyTopic(s), data.key);
      byte[] outValue = valueSerde.serializer().serialize(valueTopic(s), data.value);

      return Tuple.from(outKey, outValue).pack();
    };

    deserializer = (s, bytes) -> {
      Tuple tuple = Tuple.fromBytes(bytes);
      K key = keySerde.deserializer().deserialize(keyTopic(s), tuple.getBytes(0));
      V value = valueSerde.deserializer().deserialize(valueTopic(s), tuple.getBytes(1));
      return KeyValue.pair(key, value);
    };
  }

  private String keyTopic(String parentTopic) {
    return parentTopic + "-key-sub";
  }

  private String valueTopic(String parentTopic) {
    return parentTopic + "-value-sub";
  }

  @Override
  public Serializer<KeyValue<K, V>> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<KeyValue<K, V>> deserializer() {
    return deserializer;
  }
}

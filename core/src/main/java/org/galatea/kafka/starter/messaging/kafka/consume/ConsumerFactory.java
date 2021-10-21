package org.galatea.kafka.starter.messaging.kafka.consume;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

public interface ConsumerFactory {
  <K, V> Consumer<K, V> create(Properties consumerProps, Deserializer<K> keySerde, Deserializer<V> valueSerde);

  static ConsumerFactory defaultFactory() {
    return KafkaConsumer::new;
  }
}

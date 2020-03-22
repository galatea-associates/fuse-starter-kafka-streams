package org.galatea.kafka.starter.messaging;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

public class AvroSerdeUtil {

  public static Serde<GenericRecord> genericKeySerde(String schemaRegistryUrl) {
    Serde<GenericRecord> serde = new GenericAvroSerde();
    serde.configure(registryConfig(schemaRegistryUrl), true);
    return serde;
  }

  private static Map<String, ?> registryConfig(String schemaRegistryUrl) {
    return Collections
        .singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
  }

  public static Serde<GenericRecord> genericValueSerde(String schemaRegistryUrl) {
    Serde<GenericRecord> serde = new GenericAvroSerde();
    serde.configure(registryConfig(schemaRegistryUrl), false);
    return serde;
  }
}

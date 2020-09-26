package org.galatea.kafka.starter.testing.bean;

import static org.mockito.Mockito.mock;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.testing.bean.domain.ReplacementRule;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DefaultBeanRules {

  @Getter
  private static final SchemaRegistryClient mockRegistryClient = new MockSchemaRegistryClient();

  public static ReplacementRule topicSerdes() {
    return ReplacementRule
        .of(beanData -> beanData.getBean().getClass().equals(Topic.class), beanData -> {

          Topic<?, ?> topic = (Topic<?, ?>) beanData.getBean();
          Serde<?> keySerde = topic.getKeySerde();
          Serde<?> valueSerde = topic.getValueSerde();

          if (keySerde.getClass().equals(SpecificAvroSerde.class)) {
            keySerde = new SpecificAvroSerde<>(mockRegistryClient);
            Map<String, String> configMap = new HashMap<>();
            configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:65535");
            keySerde.configure(configMap, true);
          }
          if (valueSerde.getClass().equals(SpecificAvroSerde.class)) {
            valueSerde = new SpecificAvroSerde<>(mockRegistryClient);
            Map<String, String> configMap = new HashMap<>();
            configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:65535");
            valueSerde.configure(configMap, false);
          }

          return new Topic<>(topic.getName(), keySerde, valueSerde);
        });
  }

  public static ReplacementRule adminClient() {
    return ReplacementRule
        .of(beanData -> beanData.getBean().getClass().equals(KafkaAdminClient.class),
            beanData -> {
              KafkaAdminClient client = (KafkaAdminClient) beanData.getBean();
              client.close(Duration.ZERO);
              return mock(KafkaAdminClient.class);
            });
  }

}

package org.galatea.kafka.starter.testing.bean;

import static org.mockito.Mockito.mock;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.galatea.kafka.starter.messaging.SerdePairSupplier;
import org.galatea.kafka.starter.testing.bean.domain.ReplacementRule;
import org.unitils.util.ReflectionUtils;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DefaultBeanRules {

  public static ReplacementRule replaceSerdesWithMocks(SchemaRegistryClient mockRegistryClient) {
    return ReplacementRule.ofTyped(SerdePairSupplier.class,
        existing -> {
          SerdePairSupplier<?, ?> bean = (SerdePairSupplier<?, ?>) existing.getBean();

          if (SpecificAvroSerde.class.isAssignableFrom(bean.getKeySerde().getClass())) {
            SpecificAvroSerde<SpecificRecord> keySerde = new SpecificAvroSerde<>(
                mockRegistryClient);
            Map<String, String> configMap = new HashMap<>();
            configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:65535");
            keySerde.configure(configMap, true);

            // use reflection to set the field inside the bean
            try {
              ReflectionUtils.setFieldValue(bean, "keySerde", keySerde);
            } catch (Exception e) {
              log.error("Unable to set keySerde to mock in {}", bean);
            }
          }
          if (SpecificAvroSerde.class.isAssignableFrom(bean.getValueSerde().getClass())) {
            SpecificAvroSerde<SpecificRecord> valueSerde = new SpecificAvroSerde<>(
                mockRegistryClient);
            Map<String, String> configMap = new HashMap<>();
            configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:65535");
            valueSerde.configure(configMap, false);

            // use reflection to set the field inside the bean
            try {
              ReflectionUtils.setFieldValue(bean, "valueSerde", valueSerde);
            } catch (Exception e) {
              log.error("Unable to set keySerde to mock in {}", bean);
            }
          }

          return bean;
        });
  }

  public static ReplacementRule mockAdminClient() {
    return ReplacementRule
        .of(beanData -> beanData.getBean().getClass().equals(KafkaAdminClient.class),
            beanData -> {
              KafkaAdminClient client = (KafkaAdminClient) beanData.getBean();
              client.close(Duration.ZERO);
              return mock(KafkaAdminClient.class);
            });
  }

}

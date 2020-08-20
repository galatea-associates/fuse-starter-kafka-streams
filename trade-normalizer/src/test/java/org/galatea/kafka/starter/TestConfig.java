package org.galatea.kafka.starter;

import static org.mockito.Mockito.mock;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.starter.messaging.Topic;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestConfig {

  private SchemaRegistryClient mockRegistryClient = new MockSchemaRegistryClient();

  @Bean
  BeanPostProcessor postProcessor() {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessBeforeInitialization(Object bean, String beanName)
          throws BeansException {
        if (bean.getClass().equals(Topic.class)) {
          Topic<?, ?> topic = (Topic<?, ?>) bean;
          Serde<?> keySerde = topic.getKeySerde();
          Serde<?> valueSerde = topic.getValueSerde();
          if (keySerde.getClass().equals(SpecificAvroSerde.class)) {
            keySerde = new SpecificAvroSerde<>(mockRegistryClient);
            Map<String, String> configMap = new HashMap<>();
            configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:65535");
            keySerde.configure(configMap, true);
          }
          if (valueSerde.getClass().equals(SpecificAvroSerde.class)) {
            valueSerde = new SpecificAvroSerde<>(mockRegistryClient);
            Map<String, String> configMap = new HashMap<>();
            configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:65535");
            valueSerde.configure(configMap, false);
          }

          return new Topic<>(topic.getName(), keySerde, valueSerde);
        } else if (bean.getClass().equals(KafkaProducer.class)) {
          ((KafkaProducer) bean).close(Duration.ZERO);
          return mock(KafkaProducer.class);
        }
        return bean;
      }
    };
  }
}

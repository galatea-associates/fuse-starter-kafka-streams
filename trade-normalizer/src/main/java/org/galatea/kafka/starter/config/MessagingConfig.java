package org.galatea.kafka.starter.config;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.starter.messaging.StreamProperties;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MessagingConfig {

  @Bean
  Topic<InputTradeMsgKey, InputTradeMsgValue> inputTradeTopic(
      @Value("${messaging.topic.input.trade}") String topicName,
      @Value("${messaging.schema-registry-url}") String schemaRegistryUrl) {
    return new Topic<>(topicName, avroSerde(schemaRegistryUrl, true),
        avroSerde(schemaRegistryUrl, false));
  }

  @Bean
  Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic(
      @Value("${messaging.topic.global.security}") String topicName,
      @Value("${messaging.schema-registry-url}") String schemaRegistryUrl) {
    return new Topic<>(topicName, avroSerde(schemaRegistryUrl, true),
        avroSerde(schemaRegistryUrl, false));
  }

  @Bean
  Topic<TradeMsgKey, TradeMsgValue> normalizedTradeTopic(
      @Value("${messaging.topic.output.trade}") String topicName,
      @Value("${messaging.schema-registry-url}") String schemaRegistryUrl) {
    return new Topic<>(topicName, avroSerde(schemaRegistryUrl, true),
        avroSerde(schemaRegistryUrl, false));
  }

  private static <T extends SpecificRecord> Serde<T> avroSerde(String schemaRegistryUrl,
      boolean forKey) {
    SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();

    Map<String, String> configMap = new HashMap<>();
    configMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    serde.configure(configMap, forKey);

    return serde;
  }

  @Bean
  @ConfigurationProperties(prefix = "messaging")
  StreamProperties streamProperties() {
    return new StreamProperties();
  }
}

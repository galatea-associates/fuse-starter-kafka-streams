package org.galatea.kafka.starter.config;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.streams.GlobalStoreRef;
import org.galatea.kafka.starter.messaging.streams.TaskStoreRef;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
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
  GlobalStoreRef<SecurityIsinMsgKey, SecurityMsgValue> securityStoreRef(
      Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic) {
    return GlobalStoreRef.<SecurityIsinMsgKey, SecurityMsgValue>builder()
        .onTopic(securityTopic)
        .build();
  }

  @Bean
  TaskStoreRef<InputTradeMsgKey, InputTradeMsgValue> tradeStoreRef(
      Topic<InputTradeMsgKey, InputTradeMsgValue> tradeTopic) {
    return TaskStoreRef.<InputTradeMsgKey, InputTradeMsgValue>builder()
        .name("trade")
        .keySerde(tradeTopic.getKeySerde())
        .valueSerde(tradeTopic.getValueSerde())
        .build();
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
}

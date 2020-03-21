package org.galatea.kafka.shell.config;

import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.galatea.kafka.starter.messaging.AvroSerdeUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaInterfaceConfig {

  @Bean
  public Consumer<GenericRecord, GenericRecord> consumer(MessagingConfig config) {
    Properties props = new Properties();
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServer());

    return new KafkaConsumer<>(props,
        AvroSerdeUtil.genericKeySerde(config.getSchemaRegistryUrl()).deserializer(),
        AvroSerdeUtil.genericValueSerde(config.getSchemaRegistryUrl()).deserializer());
  }

  @Bean
  public AdminClient adminClient(MessagingConfig config) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServer());
    return KafkaAdminClient.create(props);
  }

}

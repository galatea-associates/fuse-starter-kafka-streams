package org.galatea.kafka.starter.messaging;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.galatea.kafka.starter.messaging.config.InternalKafkaConfig;
import org.galatea.kafka.starter.messaging.config.KafkaConfig;
import org.galatea.kafka.starter.messaging.config.StorePersistence;
import org.galatea.kafka.starter.messaging.config.StorePersistenceConfig;
import org.galatea.kafka.starter.messaging.streams.GStreamBuilder;
import org.galatea.kafka.starter.messaging.streams.StorePersistenceSupplier;
import org.galatea.kafka.starter.messaging.streams.TopologyProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Slf4j
@Configuration
@Import({KafkaStreamsStarter.class, StorePersistenceConfig.class})
public class KafkaStreamsAutoconfig {

  @Bean
  @ConfigurationProperties(prefix = "kafka")
  protected InternalKafkaConfig internalKafkaConfig() {
    return new InternalKafkaConfig();
  }

  @Bean
  public KafkaConfig kafkaConfig(InternalKafkaConfig inner) {
    return new KafkaConfig(inner);
  }

  @Bean
  public AdminClient kafkaAdminClient(KafkaConfig config) {
    return KafkaAdminClient.create(config.adminMap());
  }

  @Bean
  @Profile("!test")
  public StorePersistenceSupplier ymlBasedPersistenceSupplier(
      StorePersistenceConfig persistenceConfig) {
    return new ConfigBasedStorePersistenceSupplier(persistenceConfig);
  }

  @Bean
  @Profile("test")
  public StorePersistenceSupplier inMemoryPersistenceSupplier() {
    return StorePersistenceSupplier.alwaysUse(StorePersistence.IN_MEMORY);
  }

  @Bean
  @ConditionalOnBean(TopologyProvider.class)
  public Topology topology(TopologyProvider provider, StorePersistenceSupplier persistenceSupplier) {
    Topology topology = provider
        .buildTopology(new GStreamBuilder(new StreamsBuilder(), persistenceSupplier));
    log.info("{}", topology.describe());
    return topology;
  }

  @Bean
  @ConditionalOnBean(Topology.class)
  @Profile("!test")
  public KafkaStreams kafkaStreams(KafkaConfig kafkaConfig, Topology topology) {
    Properties props = kafkaConfig.streamsProperties();
    return new KafkaStreams(topology, props);
  }

}

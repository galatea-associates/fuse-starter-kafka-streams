package org.galatea.kafka.starter.messaging;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.galatea.kafka.starter.messaging.config.InternalKafkaConfig;
import org.galatea.kafka.starter.messaging.config.KafkaConfig;
import org.galatea.kafka.starter.messaging.streams.GStreamBuilder;
import org.galatea.kafka.starter.messaging.streams.TopologyProvider;
import org.galatea.kafka.starter.messaging.streams.partition.GStreamInterceptor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Slf4j
@Configuration
@Import({KafkaStreamsStarter.class})
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
  public Object setInterceptorAdminClient(AdminClient kafkaAdminClient) {
    GStreamInterceptor.setKafkaAdminClient(kafkaAdminClient);
    return new Object();
  }

  @Bean
  @ConditionalOnBean(TopologyProvider.class)
  @Profile("!test")
  @DependsOn("setInterceptorAdminClient")
  public KafkaStreams kafkaStreams(KafkaConfig kafkaConfig, TopologyProvider topologyProvider) {
    Properties props = kafkaConfig.streamsProperties();
    addProducerInterceptor(props);
    Topology topology = topologyProvider.buildTopology(new GStreamBuilder(new StreamsBuilder()));
    log.info("{}", topology.describe());
    return new KafkaStreams(topology, props);
  }

  private void addProducerInterceptor(Properties props) {
    String interceptorConfigKey = ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
    String configValue = props.getProperty(interceptorConfigKey);
    configValue = configValue != null ? "," + configValue : "";
    configValue = GStreamInterceptor.class.getName() + configValue;
    props.put(interceptorConfigKey, configValue);
  }

}

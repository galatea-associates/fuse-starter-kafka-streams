package org.galatea.kafka.shell.config;

import java.time.Duration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "messaging")
public class MessagingConfig {

  private String environmentId;
  private String bootstrapServer;
  private String schemaRegistryUrl;
  private String stateDir;
  private Duration connectTimeout;
}

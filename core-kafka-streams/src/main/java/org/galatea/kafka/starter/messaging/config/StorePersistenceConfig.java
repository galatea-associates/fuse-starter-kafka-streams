package org.galatea.kafka.starter.messaging.config;

import java.util.LinkedList;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "stores.persistence")
public class StorePersistenceConfig {

  private List<StorePersistencePattern> patterns = new LinkedList<>();
  private StorePersistence fallback = StorePersistence.ON_DISK;
}

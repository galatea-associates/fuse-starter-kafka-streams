package org.galatea.kafka.starter.messaging;

import lombok.RequiredArgsConstructor;
import org.galatea.kafka.starter.messaging.config.StorePersistence;
import org.galatea.kafka.starter.messaging.config.StorePersistenceConfig;
import org.galatea.kafka.starter.messaging.config.StorePersistencePattern;
import org.galatea.kafka.starter.messaging.streams.StorePersistenceSupplier;

@RequiredArgsConstructor
public class ConfigBasedStorePersistenceSupplier implements StorePersistenceSupplier {

  private final StorePersistenceConfig config;

  @Override
  public StorePersistence apply(String name) {
    return config.getPatterns().stream()
        .filter(patternConfig -> patternConfig.getPattern().matcher(name).find())
        .map(StorePersistencePattern::getPersistence)
        .findFirst()
        .orElse(config.getFallback());
  }
}

package org.galatea.kafka.starter.messaging.streams;

import org.galatea.kafka.starter.messaging.config.StorePersistence;

public interface StorePersistenceSupplier {

  StorePersistence apply(String name);

  static StorePersistenceSupplier alwaysUse(StorePersistence persistence) {
    return name -> persistence;
  }
}

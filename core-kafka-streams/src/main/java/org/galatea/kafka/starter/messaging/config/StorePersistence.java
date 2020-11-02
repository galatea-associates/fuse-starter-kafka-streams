package org.galatea.kafka.starter.messaging.config;

import java.util.function.Function;
import lombok.Getter;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public enum StorePersistence {
  ON_DISK(Stores::persistentKeyValueStore),
  IN_MEMORY(Stores::inMemoryKeyValueStore);

  @Getter
  private final Function<String, KeyValueBytesStoreSupplier> persistenceSupplier;
  StorePersistence(Function<String, KeyValueBytesStoreSupplier> persistenceSupplier) {
    this.persistenceSupplier = persistenceSupplier;
  }
}

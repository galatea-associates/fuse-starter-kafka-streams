package org.galatea.kafka.shell.controller;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.galatea.kafka.shell.stores.OffsetTrackingRecordStore;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RecordStoreController {

  private static final String OFFSET_STORE_NAME = "offset";
  private final RocksDbController rocksDbController;
  private RocksDB offsetDb;
  @Getter
  private final Map<String, OffsetTrackingRecordStore> stores = new HashMap<>();

  public boolean storeExist(String topicname, boolean compact) {
    return stores.containsKey(storeName(topicname, compact));
  }

  public boolean storeExist(String storeName) {
    return stores.containsKey(storeName);
  }

  private String storeName(String topic, boolean compact) {
    if (compact) {
      topic += "-compact";
    }
    return topic;
  }

  public OffsetTrackingRecordStore newStore(String topicName, boolean compact) {
    setupOffsetDb();
    String storeName = storeName(topicName, compact);
    try {
      if (stores.containsKey(storeName)) {
        log.info("Store already exists, doing nothing.");
        return stores.get(storeName);
      }
      RocksDB rocksDB = rocksDbController.newStore(storeName);
      OffsetTrackingRecordStore store = new OffsetTrackingRecordStore(storeName,
          compact, rocksDB, offsetDb);
      stores.put(storeName, store);
      return store;

    } catch (RocksDBException e) {
      log.error("Could not initialize store {}", storeName, e);
      throw new IllegalStateException(e);
    }
  }

  private void setupOffsetDb() {
    if (offsetDb == null) {
      try {
        offsetDb = rocksDbController.newStore(OFFSET_STORE_NAME);
      } catch (RocksDBException e) {
        log.error("Could not initialize Offset store {}", OFFSET_STORE_NAME, e);
        throw new IllegalStateException(e);
      }
    }
  }
}

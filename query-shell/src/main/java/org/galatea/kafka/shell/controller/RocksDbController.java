package org.galatea.kafka.shell.controller;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.galatea.kafka.shell.config.MessagingConfig;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RocksDbController implements Closeable {

  @Getter
  private final Map<String, RocksDB> stores = new HashMap<>();
  private static final Options DEFAULT_OPTIONS = new Options().setCreateIfMissing(true);

  private final MessagingConfig messagingConfig;

  public RocksDbController(MessagingConfig messagingConfig) throws IOException {
    this.messagingConfig = messagingConfig;
    // delete old stores at startup
    FileUtils.deleteDirectory(new File(messagingConfig.getStateDir()));
    // a static method that loads the RocksDB C++ library.
    RocksDB.loadLibrary();

  }

  public RocksDB newStore(String storeName) throws RocksDBException {
    String storeDir = messagingConfig.getStateDir() + "/" + fileFriendlyString(storeName);
    File dir = new File(storeDir);
    if (!dir.mkdirs()) {
      log.warn("Unable to create state dir {}", dir);
    }
    return RocksDB.open(DEFAULT_OPTIONS, storeDir);
  }

  private String fileFriendlyString(String storeName) {
    return storeName.replaceAll("[^A-Za-z0-9\\-_]", "-");
  }

  @Override
  public void close() throws IOException {
    stores.forEach((name, store) -> store.close());
  }
}

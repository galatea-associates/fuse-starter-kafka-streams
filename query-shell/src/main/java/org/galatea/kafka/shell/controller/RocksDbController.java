package org.galatea.kafka.shell.controller;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RocksDbController implements Closeable {

  @Getter
  private final Map<String, RocksDB> stores = new HashMap<>();
  private static final Options DEFAULT_OPTIONS = new Options().setCreateIfMissing(true);

  @Value("${messaging.state-dir}")
  private String stateDir;

  public RocksDbController() {
    // a static method that loads the RocksDB C++ library.
    RocksDB.loadLibrary();
    // TODO: load existing stores on startup
  }

  public RocksDB newStore(String storeName) throws RocksDBException {
    String storeDir = stateDir + "\\" + fileFriendlyString(storeName);
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

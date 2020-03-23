package org.galatea.kafka.shell.controller;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.galatea.kafka.shell.config.MessagingConfig;
import org.galatea.kafka.shell.util.FileSystemUtil;
import org.galatea.kafka.starter.util.Pair;
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
  private int lastStoreId = 0;

  public RocksDbController(MessagingConfig messagingConfig) throws IOException {
    this.messagingConfig = messagingConfig;
    // delete old stores at startup
    FileSystemUtil.deleteDirectory(new File(messagingConfig.getStateDir()));
    // a static method that loads the RocksDB C++ library.
    RocksDB.loadLibrary();
  }

  public Pair<String, RocksDB> newStore(String storeName) throws RocksDBException, IOException {
    String storeDir = stateDirFor(storeName + "-" + (++lastStoreId));
    File dir = new File(storeDir);
    FileSystemUtil.deleteDirectory(dir);
    if (!dir.mkdirs()) {
      log.warn("Unable to create state dir {}", dir);
    }
    return Pair.of(storeDir, RocksDB.open(DEFAULT_OPTIONS, storeDir));
  }

  public String stateDirFor(String storeName) {
    return messagingConfig.getStateDir() + "/" + fileFriendlyString(storeName);
  }

  private String fileFriendlyString(String storeName) {
    return storeName.replaceAll("[^A-Za-z0-9\\-_]", "-");
  }

  @Override
  public void close() throws IOException {
    stores.forEach((name, store) -> store.close());
  }
}

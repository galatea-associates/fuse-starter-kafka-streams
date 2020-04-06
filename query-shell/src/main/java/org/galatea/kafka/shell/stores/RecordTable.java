package org.galatea.kafka.shell.stores;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.shell.util.FileSystemUtil;
import org.galatea.kafka.starter.util.Pair;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

@Slf4j
@RequiredArgsConstructor
public class RecordTable<K, V> implements Closeable {

  @Getter
  private final String name;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final RocksDB db;
  private final String stateDir;
  private boolean storeOpen = true;

  public boolean isStoreOpen() {
    return storeOpen;
  }

  protected void validateStoreOpen() {
    if (!storeOpen) {
      throw new IllegalStateException(String.format("Store %s is not open", name));
    }
  }

  public Optional<V> get(K key) {
    return Optional.ofNullable(getRaw(key));
  }

  public void doWith(Predicate<Pair<K, V>> predicate, Consumer<Pair<K, V>> doWithRecord,
      long maxResults) {

    validateStoreOpen();
    RocksIterator it = db.newIterator();
    it.seekToFirst();
    long processedRecords = 0;
    while (it.isValid() && processedRecords < maxResults) {
      Pair<K, V> pair = Pair.of(deserializeKey(it.key()), deserializeValue(it.value()));
      if (predicate.test(pair)) {
        doWithRecord.accept(pair);
        processedRecords++;
      }
      it.next();
    }
    it.close();
  }

  public V put(K key, V value) {
    validateStoreOpen();
    V oldValue = null;
    try {
      byte[] serializedKey = serializeKey(key);
      oldValue = deserializeValue(db.get(serializedKey));
      db.put(serializedKey, serializeValue(value));
    } catch (RocksDBException e) {
      log.error("Could not put into store {} {}|{}", db.getName(), key, value);
      close();
    }
    return oldValue;
  }

  private V getRaw(K key) {
    validateStoreOpen();
    byte[] serializedKey = serializeKey(key);
    try {
      byte[] bytes = db.get(serializedKey);
      return deserializeValue(bytes);
    } catch (RocksDBException e) {
      log.error("Could not retrieve from store {}: using key {}", name, key, e);
      close();
      throw new IllegalStateException(e);
    }
  }

  private V deserializeValue(byte[] bytes) {

    return bytes == null ? null : valueSerde.deserializer().deserialize("", bytes);
  }

  private K deserializeKey(byte[] bytes) {
    return bytes == null ? null : keySerde.deserializer().deserialize("", bytes);
  }

  private byte[] serializeKey(K key) {
    return keySerde.serializer().serialize("", key);
  }

  private byte[] serializeValue(V value) {
    return valueSerde.serializer().serialize("", value);
  }

  @Override
  public void close() {
    storeOpen = false;
  }

  public void close(boolean purge) {
    close();
    if (purge) {
      FileSystemUtil.deleteDirectory(new File(stateDir));
    }
  }
}

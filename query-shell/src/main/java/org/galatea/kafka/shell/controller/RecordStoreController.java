package org.galatea.kafka.shell.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RecordStoreController {

  private final RocksDbController rocksDbController;
  @Getter
  private final Map<String, TableDetails> tables = new HashMap<>();
  private final Map<String, String> aliases = new HashMap<>();
  private final Serde<DbRecordKey> compactKeySerde;
  private final Serde<DbRecordKey> allRecordKeySerde;
  private final Serde<DbRecord> valueSerde;

  public ConsumerRecordTable getTable(String tableName) {
    if (tables.containsKey(tableName)) {
      return tables.get(tableName).getTable();
    } else if (aliases.containsKey(tableName) && tables.containsKey(aliases.get(tableName))) {
      return tables.get(aliases.get(tableName)).getTable();
    }
    return null;
  }

  public boolean tableExist(String topicname, boolean compact) {
    return tableExist(tableName(topicname, compact));
  }

  public boolean tableExist(String tableName) {
    return tables.containsKey(tableName) || tables.containsKey(aliases.get(tableName));
  }

  private String tableName(String topic, boolean compact) {
    if (compact) {
      topic += "-compact";
    }
    return topic;
  }

  public boolean setAlias(String tableName, String alias) {
    if (tables.containsKey(tableName)) {
      tables.get(tableName).setAlias(alias);
      aliases.put(alias, tableName);
      return true;
    } else {
      return false;
    }
  }

  public ConsumerRecordTable newTable(String topicName, boolean compact) {
    String tableName = tableName(topicName, compact);
    Serde<DbRecordKey> keySerde = this.allRecordKeySerde;
    if (compact) {
      keySerde = this.compactKeySerde;
    }
    try {
      if (tables.containsKey(tableName)) {
        log.info("Store already exists, doing nothing.");
        return tables.get(tableName).getTable();
      }
      RocksDB rocksDB = rocksDbController.newStore(tableName);
      ConsumerRecordTable table = new ConsumerRecordTable(tableName, keySerde, this.valueSerde,
          rocksDB, rocksDB.getName());
      tables.put(tableName, new TableDetails(table));
      return table;

    } catch (RocksDBException | IOException e) {
      log.error("Could not initialize store {}", tableName, e);
      throw new IllegalStateException(e);
    }
  }

  public void deleteTable(String name) {
    if (tableExist(name)) {

      TableDetails tableDetails = tables.get(name);
      if (tableDetails.getAlias() != null) {
        aliases.remove(tableDetails.getAlias());
      }
      ConsumerRecordTable table = tableDetails.getTable();
      table.close(true);
      tables.remove(name);
    }

  }

  @Getter
  @RequiredArgsConstructor
  public static class TableDetails {

    private final ConsumerRecordTable table;
    @Setter
    private String alias = null;
  }
}
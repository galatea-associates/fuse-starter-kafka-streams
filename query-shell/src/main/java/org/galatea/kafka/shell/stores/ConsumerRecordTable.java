package org.galatea.kafka.shell.stores;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.domain.StoreStatus;
import org.rocksdb.RocksDB;

@Slf4j
@Getter
public class ConsumerRecordTable extends RecordTable<DbRecordKey, DbRecord> {

  private final Map<Integer, Long> partitionLatestOffsetReceived = new HashMap<>();
  @Getter
  private long recordsReceived = 0;
  @Getter
  private long recordsInStore = 0;

  public ConsumerRecordTable(String name, Serde<DbRecordKey> keySerde, Serde<DbRecord> valueSerde,
      RocksDB db) {
    super(name, keySerde, valueSerde, db);
  }

  public StoreStatus status() {
    return new StoreStatus(recordsReceived, recordsInStore, partitionLatestOffsetReceived);
  }

  public void addRecord(KeyValue<DbRecordKey, DbRecord> record) {

    if (latestOffsetForPartition(record.value.getPartition().get()) > record.value.getOffset()
        .get()) {
      log.debug("Store {} discarding old record: {}", getName(), record);
    } else {
      log.info("Store {} received record {}", getName(), record);
      if (put(record.key, record.value) == null) {
        recordsInStore++;
      }
      recordsReceived++;
      partitionLatestOffsetReceived
          .put(record.value.getPartition().get(), record.value.getOffset().get());
    }
  }

  private Long latestOffsetForPartition(Integer partition) {
    return Optional.ofNullable(partitionLatestOffsetReceived.get(partition)).orElse(-1L);
  }
}

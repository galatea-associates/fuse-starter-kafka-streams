package org.galatea.kafka.shell.stores;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.galatea.kafka.shell.domain.StoreStatus;
import org.galatea.kafka.shell.util.RecordSerializer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

@Slf4j
@RequiredArgsConstructor
@Getter
public class OffsetTrackingRecordStore {

  private final String storeName;
  private final boolean compactStore;
  private final Map<Integer, Long> partitionLatestOffsetReceived = new HashMap<>();
  private final RocksDB underlyingDb;
  private final RocksDB offsetDb;
  private Exception thrownException = null;
  @Getter
  private long recordsReceived = 0;
  @Getter
  private long recordsInStore = 0;

  public StoreStatus status() {
    return new StoreStatus(recordsReceived, recordsInStore, partitionLatestOffsetReceived);
  }

  public void addRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {

    if (thrownException != null) {
      return;
    }

    if (latestOffsetForPartition(record.partition()) > record.offset()) {
      log.debug("Store {} discarding old record: {}", storeName, record);
    } else {
      log.info("Store {} received record {}", storeName, record);
      try {
        updateDb(underlyingDb, record);
        updateOffsetDb(offsetDb, record);
      } catch (RocksDBException e) {
        log.error("Could not write to RocksDB. Closing store {}.", storeName, e);
        thrownException = e;
      }

      partitionLatestOffsetReceived.put(record.partition(), record.offset());
    }
  }

  private void updateOffsetDb(RocksDB offsetDb,
      ConsumerRecord<GenericRecord, GenericRecord> record) throws RocksDBException {
    offsetDb.put(RecordSerializer.topicName(record.topic(), record.partition()),
        RecordSerializer.offset(record.offset()));
  }

  private void updateDb(RocksDB underlyingDb, ConsumerRecord<GenericRecord, GenericRecord> record)
      throws RocksDBException {
    recordsReceived++;
    byte[] key = RecordSerializer.key(record, compactStore);
    if (compactStore) {
      if (underlyingDb.get(key) != null) {
        recordsInStore--;
      }
    }
    recordsInStore++;
    underlyingDb.put(key, RecordSerializer.value(record));
  }

  private Long latestOffsetForPartition(Integer partition) {
    return Optional.ofNullable(partitionLatestOffsetReceived.get(partition)).orElse(-1L);
  }
}

package org.galatea.kafka.shell.stores;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.domain.StoreStatus;
import org.galatea.kafka.shell.util.DbRecordStringUtil;
import org.galatea.kafka.shell.util.RegexPredicate;
import org.rocksdb.RocksDB;

@Slf4j
@Getter
public class ConsumerRecordTable extends RecordTable<DbRecordKey, DbRecord> {

  private final Map<Integer, Long> partitionLatestOffsetReceived = new HashMap<>();
  private final RegexPredicate recordFilter;
  private final boolean compact;
  @Getter
  private long recordsReceived = 0;
  private AtomicLong recordsInStore = new AtomicLong(0);

  private final RecordInStoreCounter counter;

  public ConsumerRecordTable(String name, Serde<DbRecordKey> keySerde, Serde<DbRecord> valueSerde,
      RocksDB db, String stateDir, RegexPredicate recordFilter, boolean compact) {
    super(name, keySerde, valueSerde, db, stateDir);
    this.compact = compact;
    this.recordFilter = recordFilter;

    if (compact) {
      counter = (existingValue, newValue) -> {
        if (existingValue.getStringValue() == null && newValue.getStringValue() != null) {
          recordsInStore.incrementAndGet();
        } else if (existingValue.getStringValue() != null && newValue.getStringValue() == null) {
          recordsInStore.decrementAndGet();
        }
      };
    } else {
      counter = ((existingValue, newValue) -> {
        if (!Objects.equals(existingValue, newValue)) {
          recordsInStore.incrementAndGet();
        }
      });
    }
  }

  public StoreStatus status() {
    return new StoreStatus(recordsReceived, getRecordsInStore(), partitionLatestOffsetReceived);
  }

  public void addRecord(ConsumerRecord<DbRecordKey, DbRecord> record) {

    if (latestOffsetForPartition(record.value().getPartition().get()) > record.value().getOffset()
        .get()) {
      log.debug("Store {} discarding old record: {}", getName(), record);
    } else {
      if (recordFilter.test(DbRecordStringUtil.recordToString(record))) {
        log.info("Store {} received record {}", getName(), record);

        counter.accept(put(record.key(), record.value()), record.value());
      }
      recordsReceived++;
      partitionLatestOffsetReceived
          .put(record.value().getPartition().get(), record.value().getOffset().get());
    }
  }

  private Long latestOffsetForPartition(Integer partition) {
    return Optional.ofNullable(partitionLatestOffsetReceived.get(partition)).orElse(-1L);
  }

  public long getRecordsInStore() {
    return recordsInStore.get();
  }

  private interface RecordInStoreCounter extends BiConsumer<DbRecord, DbRecord> {

    @Override
    void accept(DbRecord existingValue, DbRecord newValue);
  }
}

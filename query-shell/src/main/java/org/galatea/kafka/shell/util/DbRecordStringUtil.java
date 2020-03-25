package org.galatea.kafka.shell.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.starter.util.Pair;

public class DbRecordStringUtil {

  public static String recordToString(DbRecordKey key, DbRecord value) {
    return "Key: " + key.getStringKey() + ", Value:" + value.getStringValue();
  }

  public static String recordToString(ConsumerRecord<DbRecordKey, DbRecord> record) {
    return recordToString(record.key(), record.value());
  }

  public static String recordToString(Pair<DbRecordKey, DbRecord> pair) {
    return recordToString(pair.getKey(), pair.getValue());
  }
}
